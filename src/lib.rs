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
    collections::{HashMap, HashSet},
    ops::Deref,
    str::FromStr,
    sync::Arc,
};

use async_std::{sync::RwLock, task::block_on};
use async_trait::async_trait;
use bytes::Bytes;
use log::{info, warn};
use mqtt::AsyncClient;
use paho_mqtt::{self as mqtt, topic, MQTT_VERSION_5, QOS_1};
use protobuf::MessageDyn;
use up_rust::{
    ComparableListener, UAttributes, UAttributesValidators, UCode, UMessage, UStatus, UUri, UUID,
};

pub mod rpc;
pub mod transport;

// URI Wildcard consts
// TODO: Remove once up-rust contains/exposes these values
const WILDCARD_AUTHORITY: &str = "*";
const WILDCARD_ENTITY_ID: u32 = 0x0000_FFFF;
const WILDCARD_ENTITY_VERSION: u32 = 0x0000_00FF;
const WILDCARD_RESOURCE_ID: u32 = 0x0000_FFFF;

// Trait that allows for a mockable Mqtt client.
#[async_trait]
pub trait MockableMqttClient: Sync + Send {
    async fn new_client(
        config: MqttConfig,
        client_id: UUID,
        on_receive: fn(Option<paho_mqtt::Message>, Arc<RwLock<HashMap<String, HashSet<ComparableListener>>>>),
        topic_map: Arc<RwLock<HashMap<String, HashSet<ComparableListener>>>>,
    ) -> Result<Self, UStatus> where Self: Sized;

    async fn publish(
        &self,
        mqtt_message: mqtt::Message,
    ) -> Result<(), UStatus>;

    async fn subscribe(
        &self,
        topic: &str,
    ) -> Result<(), UStatus>;

    async fn unsubscribe(
        &self,
        topic: &str,
    ) -> Result<(), UStatus>;
}

pub struct AsyncMqttClient {
    inner_mqtt_client: Arc<mqtt::AsyncClient>,
}

#[async_trait]
impl MockableMqttClient for AsyncMqttClient {
    /// Create a new UPClientMqtt.
    ///
    /// # Arguments
    /// * `config` - Configuration for the mqtt client.
    /// * `client_id` - Client id for the mqtt client.
    async fn new_client(
        config: MqttConfig,
        client_id: UUID,
        on_receive: fn(Option<paho_mqtt::Message>, Arc<RwLock<HashMap<String, HashSet<ComparableListener>>>>),
        topic_map: Arc<RwLock<HashMap<String, HashSet<ComparableListener>>>>,
    ) -> Result<Self, UStatus> where Self: Sized {
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
                on_receive(message, topic_map.clone())
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
            inner_mqtt_client: Arc::new(mqtt_cli),
        })
    }
    
    async fn publish(&self, mqtt_message: mqtt::Message,) -> Result<(),UStatus>  {
        self.inner_mqtt_client.publish(mqtt_message).await.map_err(|e| {
            UStatus::fail_with_code(UCode::INTERNAL, format!("Unable to publish message: {e:?}"))
        })?;

        Ok(())
    }
    
    /// Helper function for subscribing the mqtt client to a topic.
    ///
    /// # Arguments
    /// * `topic` - Topic to subscribe to.
    async fn subscribe(&self, topic: &str) -> Result<(), UStatus> {
        // QOS 1 - Delivered and received at least once
        self.inner_mqtt_client
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
        self.inner_mqtt_client.unsubscribe(topic).await.map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Unable to unsubscribe from topic: {e:?}"),
            )
        })?;

        Ok(())
    }
}

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
    mqtt_client: Arc<dyn MockableMqttClient>,
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

        let mqtt_client = AsyncMqttClient::new_client(config, client_id, UPClientMqtt::on_receive, topic_listener_map_handle).await?;

        Ok(Self {
            mqtt_client: Arc::new(mqtt_client),
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
            self.mqtt_client.subscribe(topic).await?;
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
                self.mqtt_client.unsubscribe(topic).await?;
            }
        }

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

        // Validate UAttributes before conversion.
        UAttributesValidators::get_validator_for_attributes(attributes)
            .validate(attributes)
            .map_err(|e| {
                UStatus::fail_with_code(UCode::INTERNAL, format!("Invalid uAttributes, err: {e:?}"))
            })?;

        // Iterate over all fields in UAttributes and extract protobuf field numbers and values.
        for field in attributes.descriptor_dyn().fields() {
            // Get protobuf field number as string.
            let field_proto_number = field.number().to_string();

            // If field is a either an enum or message type, process the field.
            if field.is_singular() {
                let field_val_wrapped_opt = field.get_singular(attributes);

                if let Some(field_val_wrapped) = field_val_wrapped_opt {
                    match field_val_wrapped.get_type() {
                        protobuf::reflect::RuntimeType::U32 => {
                            if let Some(u32_val) = field_val_wrapped.to_u32() {
                                properties
                                    .push_string_pair(
                                        mqtt::PropertyCode::UserProperty,
                                        &field_proto_number,
                                        &u32_val.to_string(),
                                    )
                                    .map_err(|e| {
                                        UStatus::fail_with_code(
                                            UCode::INTERNAL,
                                            format!(
                                                "Unable to create u32 mqtt property, err: {e:?}"
                                            ),
                                        )
                                    })?;
                            }
                        }
                        protobuf::reflect::RuntimeType::String => {
                            if let Some(string_val) = field_val_wrapped.to_str() {
                                properties
                                    .push_string_pair(
                                        mqtt::PropertyCode::UserProperty,
                                        &field_proto_number,
                                        string_val,
                                    )
                                    .map_err(|e| {
                                        UStatus::fail_with_code(
                                            UCode::INTERNAL,
                                            format!(
                                                "Unable to create string mqtt property, err: {e:?}"
                                            ),
                                        )
                                    })?;
                            }
                        }
                        protobuf::reflect::RuntimeType::Enum(_) => {
                            if let Some(enum_number) = field_val_wrapped.to_enum_value() {
                                properties
                                    .push_string_pair(
                                        mqtt::PropertyCode::UserProperty,
                                        &field_proto_number,
                                        &enum_number.to_string(),
                                    )
                                    .map_err(|e| {
                                        UStatus::fail_with_code(
                                            UCode::INTERNAL,
                                            format!(
                                                "Unable to create enum mqtt property, err: {e:?}"
                                            ),
                                        )
                                    })?;
                            }
                        }
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
                                            UStatus::fail_with_code(UCode::INTERNAL, format!("Unable to create uuid mqtt property, err: {e:?}"))
                                        })?;
                                    }
                                } else if message_type_name == "uuri" {
                                    let uuri_downcast = val.downcast_ref::<UUri>();
                                    if let Some(uuri) = uuri_downcast {
                                        let uuri_string: String = uuri.into();
                                        properties.push_string_pair(mqtt::PropertyCode::UserProperty, &field_proto_number, &uuri_string).map_err(|e| {
                                            UStatus::fail_with_code(UCode::INTERNAL, format!("Unable to create uuri mqtt property, err: {e:?}"))
                                        })?;
                                    }
                                } else {
                                    return Err(UStatus::fail_with_code(
                                        UCode::INTERNAL,
                                        format!("Unsupported message type: {}", message_type_name),
                                    ));
                                }
                            } else {
                                return Err(UStatus::fail_with_code(
                                    UCode::INTERNAL,
                                    format!(
                                        "Unable to process field value from uAttributes: {}",
                                        field.name()
                                    ),
                                ));
                            }
                        }
                        _ => {
                            return Err(UStatus::fail_with_code(
                                UCode::INTERNAL,
                                format!(
                                    "Unsupported protobuf field type: {}",
                                    field_val_wrapped.get_type()
                                ),
                            ));
                        }
                    }
                }
            } else {
                warn!(
                    "Unable to process non-singular field type: {}",
                    field.name()
                );
            }
        }

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

        for (key, value) in props.user_iter() {
            let protobuf_field_number: u32 = key.parse().map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Unable to parse attribute field number, err: {e:?}"),
                )
            })?;

            if let Some(field) = attributes
                .descriptor_dyn()
                .field_by_number(protobuf_field_number)
            {
                // Need to get the reflect value to properly set the field.
                let field_value = field.get_singular_field_or_default(&attributes);
                let field_type = field_value.get_type();

                let value_box = match field_type {
                    protobuf::reflect::RuntimeType::U32 => {
                        let u32_val = value.parse::<u32>().map_err(|e| {
                            UStatus::fail_with_code(
                                UCode::INTERNAL,
                                format!("Unable to parse attribute field value, err: {e:?}"),
                            )
                        })?;
                        Ok(protobuf::reflect::ReflectValueBox::U32(u32_val))
                    }
                    protobuf::reflect::RuntimeType::String => {
                        Ok(protobuf::reflect::ReflectValueBox::String(value))
                    }
                    protobuf::reflect::RuntimeType::Enum(descriptor) => {
                        let enum_val = value.parse::<i32>().map_err(|e| {
                            UStatus::fail_with_code(
                                UCode::INTERNAL,
                                format!("Unable to parse attribute field to enum, err: {e:?}"),
                            )
                        })?;
                        Ok(protobuf::reflect::ReflectValueBox::Enum(
                            descriptor.clone(),
                            enum_val,
                        ))
                    }
                    protobuf::reflect::RuntimeType::Message(descriptor) => {
                        // Get type name of message to use for downcasting.
                        let message_type_name = descriptor.name().to_ascii_lowercase();

                        // If field value can be unwrapped as a MessageRef, process the field value.
                        // Currently only set up to process `UUID` and `UURI` types. Add more as needed.
                        if message_type_name == "uuid" {
                            let uuid = UUID::from_str(&value).map_err(|e| {
                                UStatus::fail_with_code(UCode::INTERNAL, format!("Unable to parse attribute field to uuid message, err: {e:?}"))
                            })?;
                            Ok(protobuf::reflect::ReflectValueBox::Message(Box::new(uuid)))
                        } else if message_type_name == "uuri" {
                            let uuri = UUri::from_str(&value).map_err(|e| {
                                UStatus::fail_with_code(UCode::INTERNAL, format!("Unable to parse attribute field to uuri message, err: {e:?}"))
                            })?;
                            Ok(protobuf::reflect::ReflectValueBox::Message(Box::new(uuri)))
                        } else {
                            Err(UStatus::fail_with_code(
                                UCode::INTERNAL,
                                format!("Unsupported message type: {}", message_type_name),
                            ))
                        }
                    }
                    _ => Err(UStatus::fail_with_code(
                        UCode::INTERNAL,
                        format!("Unsupported protobuf field type: {}", field_type),
                    )),
                }?;

                field.set_singular_field(&mut attributes, value_box)
            } else {
                return Err(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Unable to map user property to uAttributes: {key:?} - {value:?}"),
                ))?;
            }
        }

        // Validate the reconstructed attributes
        UAttributesValidators::get_validator_for_attributes(&attributes)
            .validate(&attributes)
            .map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Unable to construct uAttributes, err: {e:?}"),
                )
            })?;

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
    use protobuf::Enum;
    use up_rust::{UListener, UMessageType, UPayloadFormat, UPriority, UUIDBuilder};

    use test_case::test_case;

    use super::*;

    /// Constants defining the protobuf field numbers for UAttributes.
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

    pub struct SimpleListener {}

    #[async_trait]
    impl UListener for SimpleListener {
        async fn on_error(&self, status: UStatus) {
            println!("Error: {:?}", status);
        }

        async fn on_receive(&self, message: UMessage) {
            println!("Received message: {:?}", message);
        }
    }

    pub struct MockMqttClient {

    }

    #[async_trait]
    impl MockableMqttClient for MockMqttClient {
        async fn new_client(
            config: MqttConfig,
            client_id: UUID,
            on_receive: fn(Option<paho_mqtt::Message>, Arc<RwLock<HashMap<String, HashSet<ComparableListener>>>>),
            topic_map: Arc<RwLock<HashMap<String, HashSet<ComparableListener>>>>,
        ) -> Result<Self, UStatus> where Self: Sized {
            Ok(Self {})
        }

        async fn publish(
            &self,
            mqtt_message: mqtt::Message,
        ) -> Result<(), UStatus> {
            Ok(())
        }

        async fn subscribe(
            &self,
            topic: &str,
        ) -> Result<(), UStatus> {
            Ok(())
        }

        async fn unsubscribe(
            &self,
            topic: &str,
        ) -> Result<(), UStatus> {
            Ok(())
        }
    }

    // Helper function used to create a UAttributes object and mqtt properties object for testing and comparison.
    #[allow(clippy::too_many_arguments)]
    fn create_test_uattributes_and_properties(
        id: Option<UUID>,
        type_: Option<UMessageType>,
        source: Option<&str>,
        sink: Option<&str>,
        priority: Option<UPriority>,
        ttl: Option<u32>,
        perm_level: Option<u32>,
        commstatus: Option<UCode>,
        reqid: Option<UUID>,
        token: Option<&str>,
        traceparent: Option<&str>,
        payload_format: Option<UPayloadFormat>,
    ) -> (UAttributes, mqtt::Properties) {
        let uattributes = create_uattributes(
            id.clone(),
            type_,
            source,
            sink,
            priority,
            ttl,
            perm_level,
            commstatus,
            reqid.clone(),
            token,
            traceparent,
            payload_format,
        );

        let properties = create_mqtt_properties(
            id,
            type_,
            source,
            sink,
            priority,
            ttl,
            perm_level,
            commstatus,
            reqid,
            token,
            traceparent,
            payload_format,
        );

        (uattributes, properties)
    }

    // Helper function to construct UAttributes object for testing.
    #[allow(clippy::too_many_arguments)]
    fn create_uattributes(
        id: Option<UUID>,
        type_: Option<UMessageType>,
        source: Option<&str>,
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

        if let Some(id) = id {
            attributes.id = Some(id).into();
        }

        if let Some(type_) = type_ {
            attributes.type_ = type_.into();
        }

        if let Some(source) = source {
            attributes.source =
                Some(UUri::from_str(source).expect("expected valid source UUri string.")).into();
        }

        if let Some(sink) = sink {
            attributes.sink =
                Some(UUri::from_str(sink).expect("expected valid sink UUri string.")).into();
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
            attributes.token = Some(token.to_string());
        }

        if let Some(traceparent) = traceparent {
            attributes.traceparent = Some(traceparent.to_string());
        }

        if let Some(payload_format) = payload_format {
            attributes.payload_format = payload_format.into();
        }

        attributes
    }

    // Helper function to create mqtt properties for testing.
    #[allow(clippy::too_many_arguments)]
    fn create_mqtt_properties(
        id: Option<UUID>,
        type_: Option<UMessageType>,
        source: Option<&str>,
        sink: Option<&str>,
        priority: Option<UPriority>,
        ttl: Option<u32>,
        perm_level: Option<u32>,
        commstatus: Option<UCode>,
        reqid: Option<UUID>,
        token: Option<&str>,
        traceparent: Option<&str>,
        payload_format: Option<UPayloadFormat>,
    ) -> mqtt::Properties {
        let mut properties = mqtt::Properties::new();

        if let Some(id_val) = id {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    ID_NUM,
                    &id_val.to_hyphenated_string(),
                )
                .unwrap();
        }
        if let Some(type_val) = type_ {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    TYPE_NUM,
                    &type_val.value().to_string(),
                )
                .unwrap();
        }
        if let Some(source_val) = source {
            properties
                .push_string_pair(mqtt::PropertyCode::UserProperty, SOURCE_NUM, source_val)
                .unwrap();
        }
        if let Some(sink_val) = sink {
            properties
                .push_string_pair(mqtt::PropertyCode::UserProperty, SINK_NUM, sink_val)
                .unwrap();
        }
        if let Some(priority_val) = priority {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    PRIORITY_NUM,
                    &priority_val.value().to_string(),
                )
                .unwrap();
        }
        if let Some(ttl_val) = ttl {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    TTL_NUM,
                    &ttl_val.to_string(),
                )
                .unwrap();
        }
        if let Some(perm_level_val) = perm_level {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    PERM_LEVEL_NUM,
                    &perm_level_val.to_string(),
                )
                .unwrap();
        }
        if let Some(commstatus_val) = commstatus {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    COMMSTATUS_NUM,
                    &commstatus_val.value().to_string(),
                )
                .unwrap();
        }
        if let Some(reqid_val) = reqid {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    REQID_NUM,
                    &reqid_val.to_hyphenated_string(),
                )
                .unwrap();
        }
        if let Some(token_val) = token {
            properties
                .push_string_pair(mqtt::PropertyCode::UserProperty, TOKEN_NUM, token_val)
                .unwrap();
        }
        if let Some(traceparent_val) = traceparent {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    TRACEPARENT_NUM,
                    traceparent_val,
                )
                .unwrap();
        }
        if let Some(payload_format_val) = payload_format {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    PAYLOAD_NUM,
                    &payload_format_val.value().to_string(),
                )
                .unwrap();
        }

        properties
    }

    #[async_std::test]
    async fn test_add_listener() {
        let listener = Arc::new(SimpleListener {});
        let expected_listener = ComparableListener::new(listener.clone());
        let topic_map = Arc::new(RwLock::new(HashMap::new()));

        let up_client = UPClientMqtt {
            mqtt_client: Arc::new(MockMqttClient {}),
            topic_listener_map: topic_map.clone(),
            authority_name: "test".to_string(),
            client_type: UPClientMqttType::Device,
        };

        assert!(topic_map.read().await.is_empty());

        let result = up_client
            .add_listener("test_topic", listener.clone())
            .await;

        assert!(result.is_ok());

        let actual_topic_map = topic_map.read().await;

        assert!(actual_topic_map.contains_key("test_topic"));
        let actual_listeners = actual_topic_map.get("test_topic").unwrap();
        assert_eq!(actual_listeners.len(), 1);
        assert!(actual_listeners.contains(&expected_listener));
    }

    #[async_std::test]
    async fn test_remove_listener() {
        let listener_1 = Arc::new(SimpleListener {});
        let comparable_listener_1 = ComparableListener::new(listener_1.clone());
        let listener_2 = Arc::new(SimpleListener {});
        let comparable_listener_2 = ComparableListener::new(listener_2.clone());
        let topic_map = Arc::new(RwLock::new(HashMap::new()));

        topic_map.write().await.insert(
            "test_topic".to_string(),
            [comparable_listener_1.clone(), comparable_listener_2.clone()]
                .iter()
                .cloned()
                .collect(),
        );

        let up_client = UPClientMqtt {
            mqtt_client: Arc::new(MockMqttClient {}),
            topic_listener_map: topic_map.clone(),
            authority_name: "test".to_string(),
            client_type: UPClientMqttType::Device,
        };

        println!("{}", topic_map.read().await.len());

        assert!(!topic_map.read().await.is_empty());

        let result = up_client
            .remove_listener("test_topic", listener_1.clone())
            .await;

        assert!(result.is_ok());

        {
            let actual_topic_map = topic_map.read().await;

            assert!(actual_topic_map.contains_key("test_topic"));
            let actual_listeners = actual_topic_map.get("test_topic").unwrap();
            assert_eq!(actual_listeners.len(), 1);
            assert!(!actual_listeners.contains(&comparable_listener_1));
            assert!(actual_listeners.contains(&comparable_listener_2));
        }

        let result = up_client
            .remove_listener("test_topic", listener_2.clone())
            .await;

        assert!(result.is_ok());
        assert!(!topic_map.read().await.is_empty());
    }

    #[test_case(create_test_uattributes_and_properties(Some(UUIDBuilder::build()), Some(UMessageType::UMESSAGE_TYPE_PUBLISH), Some("//VIN.vehicles/A8000/2/8A50"), None, None, None, None, None, None, None, None, None), 3, None; "Publish success")]
    #[test_case(create_test_uattributes_and_properties(Some(UUIDBuilder::build()), Some(UMessageType::UMESSAGE_TYPE_NOTIFICATION), Some("//VIN.vehicles/A8000/2/1A50"), Some("//VIN.vehicles/B8000/3/0"), None, None, None, None, None, None, None, None), 4, None; "Notification success")]
    #[test_case(create_test_uattributes_and_properties(Some(UUIDBuilder::build()), Some(UMessageType::UMESSAGE_TYPE_REQUEST), Some("//VIN.vehicles/A8000/2/0"), Some("//VIN.vehicles/B8000/3/1B50"), Some(UPriority::UPRIORITY_CS4), Some(3600), None, None, None, None, None, None), 6, None; "Request success")]
    #[test_case(create_test_uattributes_and_properties(Some(UUIDBuilder::build()), Some(UMessageType::UMESSAGE_TYPE_RESPONSE), Some("//VIN.vehicles/B8000/3/1B50"), Some("//VIN.vehicles/A8000/2/0"), Some(UPriority::UPRIORITY_CS4), None, None, None, Some(UUIDBuilder::build()), None, None, None), 6, None; "Response success")]
    #[test_case(create_test_uattributes_and_properties(Some(UUIDBuilder::build()), Some(UMessageType::UMESSAGE_TYPE_PUBLISH), Some("//VIN.vehicles/A8000/2/1A50"), None, None, None, None, None, None, None, None, None), 3, Some(UStatus::fail_with_code(UCode::INTERNAL, "Invalid uAttributes, err: ValidationError(\"Validation failure: Invalid source URI: Validation error: Resource ID must be >= 0x8000\")".to_string())); "Publish failure with validation error")]
    fn test_create_mqtt_properties_from_uattributes(
        (attributes, properties): (UAttributes, mqtt::Properties),
        expected_attributes_num: usize,
        expected_error: Option<UStatus>,
    ) {
        let props = UPClientMqtt::create_mqtt_properties_from_uattributes(&attributes);

        if props.is_ok() {
            let actual_props = props.unwrap();
            assert_eq!(actual_props.len(), expected_attributes_num);
            actual_props.user_iter().for_each(|(key, value)| {
                let expected_prop = properties.find_user_property(&key);
                assert_eq!(Some(value), expected_prop);
            });
        } else {
            assert_eq!(props.err(), expected_error);
        }
    }

    #[test_case(create_test_uattributes_and_properties(Some(UUIDBuilder::build()), Some(UMessageType::UMESSAGE_TYPE_PUBLISH), Some("//VIN.vehicles/A8000/2/8A50"), None, None, None, None, None, None, None, None, None), None; "Publish success")]
    #[test_case(create_test_uattributes_and_properties(Some(UUIDBuilder::build()), Some(UMessageType::UMESSAGE_TYPE_NOTIFICATION), Some("//VIN.vehicles/A8000/2/1A50"), Some("//VIN.vehicles/B8000/3/0"), None, None, None, None, None, None, None, None), None; "Notification success")]
    #[test_case(create_test_uattributes_and_properties(Some(UUIDBuilder::build()), Some(UMessageType::UMESSAGE_TYPE_REQUEST), Some("//VIN.vehicles/A8000/2/0"), Some("//VIN.vehicles/B8000/3/1B50"), Some(UPriority::UPRIORITY_CS4), Some(3600), None, None, None, None, None, None), None; "Request success")]
    #[test_case(create_test_uattributes_and_properties(Some(UUIDBuilder::build()), Some(UMessageType::UMESSAGE_TYPE_RESPONSE), Some("//VIN.vehicles/B8000/3/1B50"), Some("//VIN.vehicles/A8000/2/0"), Some(UPriority::UPRIORITY_CS4), None, None, None, Some(UUIDBuilder::build()), None, None, None), None; "Response success")]
    #[test_case(create_test_uattributes_and_properties(Some(UUIDBuilder::build()), Some(UMessageType::UMESSAGE_TYPE_PUBLISH), Some("//VIN.vehicles/A8000/2/1A50"), None, None, None, None, None, None, None, None, None), Some(UStatus::fail_with_code(UCode::INTERNAL, "Unable to construct uAttributes, err: ValidationError(\"Validation failure: Invalid source URI: Validation error: Resource ID must be >= 0x8000\")".to_string())); "Publish failure with validation error")]
    fn test_get_uattributes_from_mqtt_properties(
        (attributes, properties): (UAttributes, mqtt::Properties),
        expected_error: Option<UStatus>,
    ) {
        let attributes_result = UPClientMqtt::get_uattributes_from_mqtt_properties(&properties);

        if attributes_result.is_ok() {
            let actual_attributes = attributes_result.unwrap();
            assert_eq!(actual_attributes, attributes);
        } else {
            assert_eq!(attributes_result.err(), expected_error);
        }
    }

    #[test_case("//VIN.vehicles/A8000/2/8A50", "VIN.vehicles/A8000/2/8A50"; "Valid uuri")]
    #[test_case("A8000/2/8A50", "VIN.vehicles/A8000/2/8A50"; "Local uuri")]
    #[test_case(&format!("//{WILDCARD_AUTHORITY}/A8000/2/8A50"), "+/A8000/2/8A50"; "Wildcard authority")]
    #[test_case(&format!("//VIN.vehicles/{WILDCARD_ENTITY_ID:X}/2/8A50"), "VIN.vehicles/+/2/8A50"; "Wildcard entity id")]
    #[test_case(&format!("//VIN.vehicles/A8000/{WILDCARD_ENTITY_VERSION:X}/8A50"), "VIN.vehicles/A8000/+/8A50"; "Wildcard entity version")]
    #[test_case(&format!("//VIN.vehicles/A8000/2/{WILDCARD_RESOURCE_ID:X}"), "VIN.vehicles/A8000/2/+"; "Wildcard resource id")]

    fn test_uri_to_mqtt_topic_segment(uuri: &str, expected_segment: &str) {
        let uuri = UUri::from_str(uuri).expect("expected valid UUri string.");

        println!("{:?}", uuri);

        let up_client = UPClientMqtt {
            mqtt_client: Arc::new(MockMqttClient {}),
            topic_listener_map: Arc::new(RwLock::new(HashMap::new())),
            authority_name: "VIN.vehicles".to_string(),
            client_type: UPClientMqttType::Device,
        };

        let actual_segment = up_client.uri_to_mqtt_topic_segment(&uuri);

        assert_eq!(&actual_segment, expected_segment);
    }


    #[test_case("//VIN.vehicles/A8000/2/8A50", None, UPClientMqttType::Device, "d/VIN.vehicles/A8000/2/8A50"; "Subscribe to a specific publish topic")]
    #[test_case("//VIN.vehicles/A8000/2/8A50", Some("//VIN.vehicles/B8000/3/0"), UPClientMqttType::Device, "d/VIN.vehicles/A8000/2/8A50/VIN.vehicles/B8000/3/0"; "Subscribe to a specific notification topic")]
    #[test_case("//VIN.vehicles/A8000/2/0", Some("//VIN.vehicles/B8000/3/1B50"), UPClientMqttType::Device, "d/VIN.vehicles/A8000/2/0/VIN.vehicles/B8000/3/1B50"; "Request from device")]
    #[test_case("//VIN.vehicles/B8000/3/1B50", Some("//VIN.vehicles/A8000/2/0"), UPClientMqttType::Device, "d/VIN.vehicles/B8000/3/1B50/VIN.vehicles/A8000/2/0"; "Response from device")]
    #[test_case(&format!("//{WILDCARD_AUTHORITY}/{WILDCARD_ENTITY_ID:X}/{WILDCARD_ENTITY_VERSION:X}/{WILDCARD_RESOURCE_ID:X}"), Some("//VIN.vehicles/AB34/1/12CD"), UPClientMqttType::Device, "d/+/+/+/+/VIN.vehicles/AB34/1/12CD"; "Subscribe to incoming requests for a specific method")]
    #[test_case(&format!("//{WILDCARD_AUTHORITY}/{WILDCARD_ENTITY_ID:X}/{WILDCARD_ENTITY_VERSION:X}/{WILDCARD_RESOURCE_ID:X}"), Some(&format!("//VIN.vehicles/{WILDCARD_ENTITY_ID:X}/{WILDCARD_ENTITY_VERSION:X}/{WILDCARD_RESOURCE_ID:X}")), UPClientMqttType::Cloud, "c/+/+/+/+/VIN.vehicles/+/+/+"; "Subscribe to all incoming messages to a UAuthority in the cloud")]
    #[test_case(&format!("//VIN.vehicles/{WILDCARD_ENTITY_ID:X}/{WILDCARD_ENTITY_VERSION:X}/{WILDCARD_RESOURCE_ID:X}"), None, UPClientMqttType::Device, "d/VIN.vehicles/+/+/+"; "Subscribe to all publish messages from a different UAuthority")]
    #[test_case(&format!("//{WILDCARD_AUTHORITY}/{WILDCARD_ENTITY_ID:X}/{WILDCARD_ENTITY_VERSION:X}/{WILDCARD_RESOURCE_ID:X}"), Some(&format!("//VIN.vehicles/{WILDCARD_ENTITY_ID:X}/{WILDCARD_ENTITY_VERSION:X}/0")), UPClientMqttType::Cloud, "c/+/+/+/+/VIN.vehicles/+/+/0"; "Streamer subscribe to all notifications, requests and responses to its device from the cloud")]
    #[test_case(&format!("//{WILDCARD_AUTHORITY}/{WILDCARD_ENTITY_ID:X}/{WILDCARD_ENTITY_VERSION:X}/{WILDCARD_RESOURCE_ID:X}"), None, UPClientMqttType::Device, "d/+/+/+/+"; "Subscribe to all publish messages from devices")]
    #[test_case(&format!("//VIN.vehicles/{WILDCARD_ENTITY_ID:X}/{WILDCARD_ENTITY_VERSION:X}/{WILDCARD_RESOURCE_ID:X}"), Some(&format!("//{WILDCARD_AUTHORITY}/{WILDCARD_ENTITY_ID:X}/{WILDCARD_ENTITY_VERSION:X}/{WILDCARD_RESOURCE_ID:X}")), UPClientMqttType::Device, "d/VIN.vehicles/+/+/+/+/+/+/+"; "Subscribe to all message types but publish messages sent from a UAuthority")]
    fn test_to_mqtt_topic_string(src_uri: &str, sink_uri: Option<&str>, client_type: UPClientMqttType, expected_topic: &str) {
        let src_uri = UUri::from_str(src_uri).expect("expected valid source UUri string.");
        let sink_uri = sink_uri.map(|uri| UUri::from_str(uri).expect("expected valid sink UUri string."));

        let up_client = UPClientMqtt {
            mqtt_client: Arc::new(MockMqttClient {}),
            topic_listener_map: Arc::new(RwLock::new(HashMap::new())),
            authority_name: "VIN.vehicles".to_string(),
            client_type,
        };

        let actual_topic = up_client.to_mqtt_topic_string(&src_uri, sink_uri.as_ref());

        assert_eq!(actual_topic, expected_topic);

    }
}
