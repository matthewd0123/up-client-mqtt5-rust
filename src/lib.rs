/********************************************************************************
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use std::{
    collections::{HashMap, HashSet}, ops::Deref, str::FromStr, sync::Arc
};

use async_std::{sync::RwLock, task::block_on};
use bytes::Bytes;
use log::{info, warn};
use mqtt::AsyncClient;
use paho_mqtt::{self as mqtt, MQTT_VERSION_5, QOS_1};
use protobuf::{Enum, MessageDyn};
use up_rust::{ComparableListener, UAttributes, UCode, UMessage, UPriority, UStatus, UUri, UUID};

pub mod rpc;
pub mod transport;

/// Constants defining the protobuf field numbers for UAttributes.
// TODO: Convert this to a dynamically generated list.
pub const ID_NUM: &str = "1";
pub const TYPE_NUM: &str = "2";
pub const SOURCE_NUM: &str = "3";
pub const SINK_NUM: &str = "4";
pub const PRIORITY_NUM: &str = "5";
pub const TTL_NUM: &str = "6";
pub const PERM_LEVEL_NUM: &str = "7";
pub const COMMSTATUS_NUM: &str = "8";
pub const REQID_NUM: &str = "9";
pub const TOKEN_NUM: &str = "10";
pub const TRACEPARENT_NUM: &str = "11";
pub const PAYLOAD_NUM: &str = "12";

// URI Wildcard consts
// TODO: Remove once up-rust contains/exposes these values
const WILDCARD_AUTHORITY: &str = "*";
const WILDCARD_ENTITY_ID: u32 = 0x0000_FFFF;
const WILDCARD_ENTITY_VERSION: u32 = 0x0000_00FF;
const WILDCARD_RESOURCE_ID: u32 = 0x0000_FFFF;

/// Configuration for the mqtt client.
pub struct MqttConfig {
    /// Port of the mqtt broker to connect to.
    pub mqtt_port: String,
    /// Hostname of the mqtt broker.
    pub mqtt_hostname: String,
    /// Max buffered messages for the mqtt client.
    pub max_buffered_messages: i32,
    /// Session Expiry Interval for the mqtt client.
    pub session_expiry_interval: i32,
    /// Optional SSL options for the mqtt connection.
    pub ssl_options: Option<mqtt::SslOptions>,
}

/// UP Client for mqtt.
pub struct UPClientMqtt {
    mqtt_client: Arc<mqtt::AsyncClient>,
    topic_listener_map: Arc<RwLock<HashMap<String, HashSet<ComparableListener>>>>,
    // My authority
    authority_name: String,
    // Indicates where client instance is running.
    client_type: UPClientMqttType,
}

pub enum UPClientMqttType {
    Device,
    Cloud,
}

#[allow(dead_code)]
impl UPClientMqtt {
    /// Create a new UPClientMqtt.
    ///
    /// # Arguments
    /// * `config` - Configuration for the mqtt client.
    /// * `client_id` - Client id for the mqtt client.
    pub async fn new(
        config: MqttConfig,
        client_id: UUID,
        authority_name: String,
        client_type: UPClientMqttType,
    ) -> Result<Self, UStatus> {
        let topic_listener_map = Arc::new(RwLock::new(HashMap::new()));

        let topic_listener_map_handle = topic_listener_map.clone();

        let mqtt_protocol = if config.ssl_options.is_some() {
            "mqtts"
        } else {
            "mqtt"
        };

        let mqtt_uri = format!(
            "{}://{}:{}",
            mqtt_protocol, config.mqtt_hostname, config.mqtt_port
        );

        let mqtt_cli = mqtt::CreateOptionsBuilder::new()
            .server_uri(mqtt_uri)
            .client_id(client_id)
            .max_buffered_messages(config.max_buffered_messages)
            .create_client()
            .map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Unable to create mqtt client: {e:?}"),
                )
            })?;

        mqtt_cli.set_message_callback(
            move |_cli: &AsyncClient, message: Option<paho_mqtt::Message>| {
                UPClientMqtt::on_receive(message, topic_listener_map_handle.clone())
            },
        );

        // TODO: Integrate ssl options when connecting, may need a username, etc.
        let conn_opts = mqtt::ConnectOptionsBuilder::with_mqtt_version(MQTT_VERSION_5)
            .clean_start(false)
            .properties(mqtt::properties![mqtt::PropertyCode::SessionExpiryInterval => config.session_expiry_interval])
            .finalize();

        mqtt_cli.connect(conn_opts).await.map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Unable to connect to mqtt broker: {e:?}"),
            )
        })?;

        Ok(Self {
            mqtt_client: Arc::new(mqtt_cli),
            topic_listener_map,
            authority_name,
            client_type,
        })
    }

    /// Helper function that handles MQTT messages on reception.
    ///
    /// # Arguments
    /// * `message` - MQTT message received.
    fn on_receive(
        message: Option<paho_mqtt::Message>,
        topic_map: Arc<RwLock<HashMap<String, HashSet<ComparableListener>>>>,
    ) {
        if let Some(msg) = message {
            let topic = msg.topic();

            // Get attributes from mqtt header.
            let uattributes =
                UPClientMqtt::get_uattributes_from_mqtt_properties(msg.properties()).unwrap();

            let payload = msg.payload();
            let upayload = payload.to_vec();

            let umessage = UMessage {
                attributes: Some(uattributes).into(),
                payload: Some(upayload.into()),
                ..Default::default()
            };

            // Create UMessage from UAttributes and UPayload.
            let topic_map_read = block_on(topic_map.read());
            let listeners = topic_map_read.get(topic);

            if let Some(listeners) = listeners {
                for listener in listeners {
                    let umsg_clone = umessage.clone(); // need to clone outside of closure.
                    block_on(listener.on_receive(umsg_clone));
                }
            }
        }
    }

    /// Get the client id of the mqtt client.
    fn get_client_id(&self) -> String {
        self.mqtt_client.client_id()
    }

    /// Get the client indicator based on client type.
    fn get_client_indicator(&self) -> String {
        match self.client_type {
            UPClientMqttType::Device => "d",
            UPClientMqttType::Cloud => "c",
        }
        .to_string()
    }

    /// Send UMessage to mqtt topic.
    ///
    /// # Arguments
    /// * `topic` - Mqtt topic to send message to.
    /// * `attributes` - UAttributes to send with message.
    /// * `payload` - UMessage payload to send.
    async fn send_message(
        &self,
        topic: &str,
        attributes: &UAttributes,
        payload: Option<Bytes>,
    ) -> Result<(), UStatus> {
        let props = UPClientMqtt::create_mqtt_properties_from_uattributes(attributes)?;

        let mut msg_builder = mqtt::MessageBuilder::new()
            .topic(topic)
            .properties(props)
            .qos(QOS_1); // QOS 1 - Delivered and received at least once

        // If there is payload to send, add it to the message.
        if let Some(data) = payload {
            msg_builder = msg_builder.payload(data);
        }
        
        let msg = msg_builder.finalize();

        info!("Sending message: {:?}", msg);

        self.mqtt_client.publish(msg).await.map_err(|e| {
            UStatus::fail_with_code(UCode::INTERNAL, format!("Unable to publish message: {e:?}"))
        })?;

        Ok(())
    }

    /// Add a UListener to an mqtt topic.
    ///
    /// # Arguments
    /// * `topic` - Topic to add the listener to.
    /// * `listener` - Listener to call when the topic recieves a message.
    async fn add_listener(
        &self,
        topic: &str,
        listener: Arc<dyn up_rust::UListener>,
    ) -> Result<(), UStatus> {
        let mut topic_listener_map = self.topic_listener_map.write().await;

        if !topic_listener_map.contains_key(topic) {
            // Subscribe to topic.
            self.subscribe(topic).await?;
        }

        let listeners = topic_listener_map
            .entry(topic.to_string())
            .or_insert(HashSet::new());

        // Add listener to hash set.
        let comp_listener = ComparableListener::new(listener);
        listeners.insert(comp_listener);

        Ok(())
    }

    /// Remove a UListener from an mqtt topic.
    ///
    /// # Arguments
    /// * `topic` - Topic to remove the listener from.
    /// * `listener` - Listener to remove from the topic subscription list.
    async fn remove_listener(
        &self,
        topic: &str,
        listener: Arc<dyn up_rust::UListener>,
    ) -> Result<(), UStatus> {
        let mut topic_listener_map = self.topic_listener_map.write().await;

        if topic_listener_map.contains_key(topic) {
            topic_listener_map
                .entry(topic.to_string())
                .and_modify(|listeners| {
                    // Remove listener from hash set.
                    let comp_listener = ComparableListener::new(listener);
                    listeners.remove(&comp_listener);
                });

            // Remove topic if no listeners are left.
            if topic_listener_map.get(topic).unwrap().is_empty() {
                topic_listener_map.remove(topic);

                // Unsubscribe from topic.
                self.unsubscribe(topic).await?;
            }
        }

        Ok(())
    }

    /// Helper function for subscribing the mqtt client to a topic.
    ///
    /// # Arguments
    /// * `topic` - Topic to subscribe to.
    async fn subscribe(&self, topic: &str) -> Result<(), UStatus> {
        // QOS 1 - Delivered and received at least once
        self.mqtt_client
            .subscribe(topic, QOS_1)
            .await
            .map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Unable to subscribe to topic: {e:?}"),
                )
            })?;

        Ok(())
    }

    /// Helper function for unsubscribing the mqtt client from a topic.
    ///
    /// # Arguments
    /// * `topic` - Topic to unsubscribe from.
    async fn unsubscribe(&self, topic: &str) -> Result<(), UStatus> {
        self.mqtt_client.unsubscribe(topic).await.map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Unable to unsubscribe from topic: {e:?}"),
            )
        })?;

        Ok(())
    }

    /// Create mqtt header properties from UAttributes information.
    ///
    /// # Arguments
    /// * `attributes` - UAttributes to create properties from.
    fn create_mqtt_properties_from_uattributes(
        attributes: &UAttributes,
    ) -> Result<mqtt::Properties, UStatus> {
        let mut properties = mqtt::Properties::new();

        // If priority code is unspecified, default to CS1.
        let mut attributes_mut = attributes.to_owned();
        let priority_value = attributes_mut.priority.enum_value().map_err(|e| {
            UStatus::fail_with_code(UCode::INTERNAL, format!("Unable to parse priority: {e:?}"))
        })?;
        if priority_value == UPriority::UPRIORITY_UNSPECIFIED {
            attributes_mut.priority = UPriority::UPRIORITY_CS1.into();
        }

        // Iterate over all fields in UAttributes and extract protobuf field numbers and values.
        for field in attributes_mut.descriptor_dyn().fields() {
            // Get protobuf field number as string.
            let field_proto_number = field.number().to_string();

            // If field is a either an enum or message type, process the field.
            if field.is_singular() {
                let field_val_wrapped_opt = field.get_singular(&attributes_mut);

                if let Some(field_val_wrapped) = field_val_wrapped_opt {
                    match field_val_wrapped.get_type() {
                        protobuf::reflect::RuntimeType::U32 => {
                            if let Some(u32_val) = field_val_wrapped.to_u32() {
                                properties.push_string_pair(mqtt::PropertyCode::UserProperty, &field_proto_number, &u32_val.to_string()).map_err(|e| {
                                    UStatus::fail_with_code(UCode::INTERNAL, format!("Unable to create a u32 mqtt property: {e:?}"))
                                })?;
                            }
                        },
                        protobuf::reflect::RuntimeType::String => {
                            if let Some(string_val) = field_val_wrapped.to_str() {
                                properties.push_string_pair(mqtt::PropertyCode::UserProperty, &field_proto_number, string_val).map_err(|e| {
                                    UStatus::fail_with_code(UCode::INTERNAL, format!("Unable to create a string mqtt property: {e:?}"))
                                })?;
                            }
                        },
                        protobuf::reflect::RuntimeType::VecU8 => todo!(),
                        protobuf::reflect::RuntimeType::Enum(_) => {
                            if let Some(enum_number) = field_val_wrapped.to_enum_value() {
                                properties.push_string_pair(mqtt::PropertyCode::UserProperty, &field_proto_number, &enum_number.to_string()).map_err(|e| {
                                    UStatus::fail_with_code(UCode::INTERNAL, format!("Unable to create an enum mqtt property: {e:?}"))
                                })?;
                            }
                        },
                        protobuf::reflect::RuntimeType::Message(descriptor) => {
                            // Get type name of message to use for downcasting.
                            let message_type_name = descriptor.name().to_ascii_lowercase();

                            // If field value can be unwrapped as a MessageRef, process the field value.
                            // Currently only set up to process `UUID` and `UURI` types. Add more as needed.
                            if let Some(field_val) = field_val_wrapped.to_message() {
                                let val = field_val.deref();

                                if message_type_name == "uuid" {
                                    let uuid_downcast = val.downcast_ref::<UUID>();
                                    if let Some(uuid) = uuid_downcast {
                                        properties.push_string_pair(mqtt::PropertyCode::UserProperty, &field_proto_number, &uuid.to_hyphenated_string()).map_err(|e| {
                                            UStatus::fail_with_code(UCode::INTERNAL, format!("Unable to create a uuid mqtt property: {e:?}"))
                                        })?;
                                    }
                                } else if message_type_name == "uuri" {
                                    let uuri_downcast = val.downcast_ref::<UUri>();
                                    if let Some(uuri) = uuri_downcast {
                                        let uuri_string: String = uuri.into();
                                        properties.push_string_pair(mqtt::PropertyCode::UserProperty, &field_proto_number, &uuri_string).map_err(|e| {
                                            UStatus::fail_with_code(UCode::INTERNAL, format!("Unable to create a uuri mqtt property: {e:?}"))
                                        })?;
                                    }
                                } else {
                                    return Err(UStatus::fail_with_code(UCode::INTERNAL, format!("Unsupported message type: {}", message_type_name)));
                                }
                            } else {
                                return Err(UStatus::fail_with_code(UCode::INTERNAL, format!("Unable to process a field value from uAttributes: {}", field.name())));
                            }
                        },
                        _ => {
                            return Err(UStatus::fail_with_code(UCode::INTERNAL, format!("Unsupported protobuf field type: {}", field_val_wrapped.get_type())));
                        },
                    }
                }
            } else {
                warn!("Unable to process non-singular field type: {}", field.name());
            }
        };

        Ok(properties)
    }

    /// Get UAttributes from mqtt header properties.
    ///
    /// # Arguments
    /// * `props` - Mqtt properties to get UAttributes from.
    fn get_uattributes_from_mqtt_properties(
        props: &mqtt::Properties,
    ) -> Result<UAttributes, UStatus> {
        let mut attributes = UAttributes::default();

        props.user_iter().for_each(|(key, value)| {
            match key.as_str() {
                ID_NUM => {
                    let id = UUID::from_str(&value)
                        .map_err(|_| {
                            UStatus::fail_with_code(
                                UCode::INTERNAL,
                                "Unable to parse id from mqtt properties",
                            )
                        })
                        .unwrap();
                    attributes.id = Some(id).into();
                }
                TYPE_NUM => {
                    let type_val = value
                        .parse::<i32>()
                        .map_err(|_| {
                            UStatus::fail_with_code(
                                UCode::INTERNAL,
                                "Unable to parse type from mqtt properties",
                            )
                        })
                        .unwrap();
                    attributes.type_ = up_rust::UMessageType::from_i32(type_val).unwrap().into();
                }
                SOURCE_NUM => {
                    let source = UUri::from_str(&value)
                        .map_err(|_| {
                            UStatus::fail_with_code(
                                UCode::INTERNAL,
                                "Unable to parse source from mqtt properties",
                            )
                        })
                        .unwrap();
                    attributes.source = Some(source).into();
                }
                SINK_NUM => {
                    let sink = UUri::from_str(&value)
                        .map_err(|_| {
                            UStatus::fail_with_code(
                                UCode::INTERNAL,
                                "Unable to parse sink from mqtt properties",
                            )
                        })
                        .unwrap();
                    attributes.sink = Some(sink).into();
                }
                PRIORITY_NUM => {
                    let priority_val = value
                    .parse::<i32>()
                    .map_err(|_| {
                        UStatus::fail_with_code(
                            UCode::INTERNAL,
                            "Unable to parse priority from mqtt properties",
                        )
                    })
                    .unwrap();
                    println!("Priority: {priority_val:?}");
                    attributes.priority = UPriority::from_i32(priority_val).unwrap().into();
                }
                TTL_NUM => {
                    let ttl = value
                        .parse::<u32>()
                        .map_err(|_| {
                            UStatus::fail_with_code(
                                UCode::INTERNAL,
                                "Unable to parse ttl from mqtt properties",
                            )
                        })
                        .unwrap();
                    attributes.ttl = Some(ttl);
                }
                PERM_LEVEL_NUM => {
                    let perm_level = value
                        .parse::<u32>()
                        .map_err(|_| {
                            UStatus::fail_with_code(
                                UCode::INTERNAL,
                                "Unable to parse permission level from mqtt properties",
                            )
                        })
                        .unwrap();
                    attributes.permission_level = Some(perm_level);
                }
                COMMSTATUS_NUM => {
                    let commstatus_val = value
                        .parse::<i32>()
                        .map_err(|_| {
                            UStatus::fail_with_code(
                                UCode::INTERNAL,
                                "Unable to parse commstatus from mqtt properties",
                            )
                        })
                        .unwrap();
                    attributes.commstatus = Some(UCode::from_i32(commstatus_val).unwrap().into());
                }
                REQID_NUM => {
                    let reqid = UUID::from_str(&value)
                        .map_err(|_| {
                            UStatus::fail_with_code(
                                UCode::INTERNAL,
                                "Unable to parse reqid from mqtt properties",
                            )
                        })
                        .unwrap();
                    attributes.reqid = Some(reqid).into();
                }
                TOKEN_NUM => {
                    attributes.token = Some(value);
                }
                TRACEPARENT_NUM => {
                    attributes.traceparent = Some(value);
                }
                PAYLOAD_NUM => {
                    let payload_fmt_val = value
                        .parse::<i32>()
                        .map_err(|_| {
                            UStatus::fail_with_code(
                                UCode::INTERNAL,
                                "Unable to parse payload format from mqtt properties",
                            )
                        })
                        .unwrap();
                    attributes.payload_format = up_rust::UPayloadFormat::from_i32(payload_fmt_val)
                        .unwrap()
                        .into();
                }
                _ => {
                    //TODO: Handle unknown user props}
                    println!("Unknown user property: {key:?} - {value:?}");
                }
            }
        });

        Ok(attributes)
    }

    /// Convert a UUri to a valid mqtt topic segment.
    ///
    /// # Arguments
    /// * `uri` - UUri to convert to mqtt topic segment.
    fn uri_to_mqtt_topic_segment(&self, uri: &UUri) -> String {
        let authority = if uri.authority_name.is_empty() {
            &self.authority_name
        } else if uri.authority_name == WILDCARD_AUTHORITY {
            "+"
        } else {
            &uri.authority_name
        };

        let ue_id = if uri.ue_id == WILDCARD_ENTITY_ID {
            "+".into()
        } else {
            format!("{:X}", uri.ue_id)
        };

        let ue_ver = if uri.ue_version_major == WILDCARD_ENTITY_VERSION {
            "+".into()
        } else {
            format!("{:X}", uri.ue_version_major)
        };

        let res_id = if uri.resource_id == WILDCARD_RESOURCE_ID {
            "+".into()
        } else {
            format!("{:X}", uri.resource_id)
        };

        format!("{authority}/{ue_id}/{ue_ver}/{res_id}")
    }

    /// Create a valid mqtt topic based on a source and sink UUri.
    ///
    /// # Arguments
    /// * `src_uri` - Source UUri.
    /// * `sink_uri` - Optional sink UUri.
    fn to_mqtt_topic_string(&self, src_uri: &UUri, sink_uri: Option<&UUri>) -> String {
        let cli_indicator = &self.get_client_indicator();
        let src_segment = &self.uri_to_mqtt_topic_segment(src_uri);
        if let Some(sink) = sink_uri {
            let sink_segment = self.uri_to_mqtt_topic_segment(sink);
            return format!("{cli_indicator}/{src_segment}/{sink_segment}");
        }

        format!("{cli_indicator}/{src_segment}")
    }
}

#[cfg(test)]
mod tests {
    use up_rust::{UAttributesValidators, UMessageType, UPayloadFormat, UPriority, UUIDBuilder};

    use test_case::test_case;

    use super::*;

    // Helper function to construct UAttributes object for testing. This includes uAttributes that are invalid.
    fn create_uattributes(
        id: UUID,
        type_: UMessageType,
        source: &str,
        sink: Option<&str>,
        priority: Option<UPriority>,
        ttl: Option<u32>,
        perm_level: Option<u32>,
        commstatus: Option<UCode>,
        reqid: Option<UUID>,
        token: Option<&str>,
        traceparent: Option<&str>,
        payload_format: Option<UPayloadFormat>,
    ) -> UAttributes {
        let mut attributes = UAttributes::default();

        attributes.id = Some(id).into();
        attributes.type_ = type_.into();
        attributes.source = Some(UUri::from_str(source).expect("expected valid source UUri string.")).into();

        if let Some(sink) = sink {
            attributes.sink = Some(UUri::from_str(sink).expect("expected valid sink UUri string.")).into();
        }

        if let Some(priority) = priority {
            attributes.priority = priority.into();
        }

        if let Some(ttl) = ttl {
            attributes.ttl = Some(ttl);
        }

        if let Some(perm_level) = perm_level {
            attributes.permission_level = Some(perm_level);
        }

        if let Some(commstatus) = commstatus {
            attributes.commstatus = Some(commstatus.into());
        }

        if let Some(reqid) = reqid {
            attributes.reqid = Some(reqid).into();
        }

        if let Some(token) = token {
            attributes.token = Some(token.to_string()).into();
        }

        if let Some(traceparent) = traceparent {
            attributes.traceparent = Some(traceparent.to_string()).into();
        }

        if let Some(payload_format) = payload_format {
            attributes.payload_format = payload_format.into();
        }

        attributes
    }

    // Note: If UPriority is not set in the test case, it will be set to UPriority::UPRIORITY_CS1 and be counted towards the expected attributes total.
    #[test_case(create_uattributes(UUIDBuilder::build(), UMessageType::UMESSAGE_TYPE_PUBLISH, "//VIN.vehicles/A8000/2/1A50", None, None, None, None, None, None, None, None, None), 4, None; "Publish UAttributes success")]
    #[test_case(create_uattributes(UUIDBuilder::build(), UMessageType::UMESSAGE_TYPE_NOTIFICATION, "//VIN.vehicles/A8000/2/1A50", Some("//VIN.vehicles/B8000/3/1B50"), None, None, None, None, None, None, None, None), 5, None; "Notification UAttributes success")]
    #[test_case(create_uattributes(UUIDBuilder::build(), UMessageType::UMESSAGE_TYPE_REQUEST, "//VIN.vehicles/A8000/2/1A50", Some("//VIN.vehicles/B8000/3/1B50"), Some(UPriority::UPRIORITY_CS1), Some(3600), None, None, None, None, None, None), 6, None; "Request UAttributes success")]
    #[test_case(create_uattributes(UUIDBuilder::build(), UMessageType::UMESSAGE_TYPE_REQUEST, "//VIN.vehicles/A8000/2/1A50", Some("//VIN.vehicles/B8000/3/1B50"), Some(UPriority::UPRIORITY_CS1), None, None, None, Some(UUIDBuilder::build()), None, None, None), 6, None; "Response UAttributes success")]
    fn test_create_mqtt_properties_from_uattributes(
        attributes: UAttributes,
        expected_attributes_num: usize,
        expected_error: Option<UStatus>,
    ) {
        let props = UPClientMqtt::create_mqtt_properties_from_uattributes(&attributes);

        if props.is_ok() {
            assert_eq!(props.unwrap().len(), expected_attributes_num);
        } else {
            assert_eq!(props.err(), expected_error);
        }
    }

    #[async_std::test]
    async fn test_get_uattributes_from_mqtt_properties() {
        let mut properties = mqtt::Properties::new();

        properties
            .push_string_pair(
                mqtt::PropertyCode::UserProperty,
                ID_NUM,
                &UUIDBuilder::build().to_hyphenated_string(),
            )
            .unwrap();
        properties
            .push_string_pair(
                mqtt::PropertyCode::UserProperty,
                TYPE_NUM,
                &UMessageType::UMESSAGE_TYPE_NOTIFICATION.value().to_string(),
            )
            .unwrap();
        properties
            .push_string_pair(
                mqtt::PropertyCode::UserProperty,
                SOURCE_NUM,
                "//VIN.vehicles/A8000/2/1A50",
            )
            .unwrap();
        properties
            .push_string_pair(
                mqtt::PropertyCode::UserProperty,
                SINK_NUM,
                "//VIN.vehicles/B8000/3/0",
            )
            .unwrap();
        properties
            .push_string_pair(
                mqtt::PropertyCode::UserProperty,
                PRIORITY_NUM,
                &UPriority::UPRIORITY_CS3.value().to_string(),
            )
            .unwrap();
        properties
            .push_string_pair(mqtt::PropertyCode::UserProperty, TTL_NUM, "100")
            .unwrap();
        properties
            .push_string_pair(mqtt::PropertyCode::UserProperty, PERM_LEVEL_NUM, "1")
            .unwrap();
        properties
            .push_string_pair(
                mqtt::PropertyCode::UserProperty,
                COMMSTATUS_NUM,
                &UCode::INTERNAL.value().to_string(),
            )
            .unwrap();
        properties
            .push_string_pair(
                mqtt::PropertyCode::UserProperty,
                REQID_NUM,
                &UUIDBuilder::build().to_hyphenated_string(),
            )
            .unwrap();
        properties
            .push_string_pair(mqtt::PropertyCode::UserProperty, TOKEN_NUM, "token")
            .unwrap();
        properties
            .push_string_pair(
                mqtt::PropertyCode::UserProperty,
                TRACEPARENT_NUM,
                "traceparent",
            )
            .unwrap();
        properties
            .push_string_pair(
                mqtt::PropertyCode::UserProperty,
                PAYLOAD_NUM,
                &up_rust::UPayloadFormat::UPAYLOAD_FORMAT_TEXT
                    .value()
                    .to_string(),
            )
            .unwrap();

        let attributes = UPClientMqtt::get_uattributes_from_mqtt_properties(&properties).unwrap();

        assert!(
            UAttributesValidators::get_validator_for_attributes(&attributes)
                .validate(&attributes)
                .is_ok()
        );
    }
}

