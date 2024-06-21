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

use std::sync::Arc;

use async_trait::async_trait;

use up_rust::{UAttributesValidators, UCode, UListener, UMessage, UStatus, UTransport, UUri};

use crate::UPClientMqtt;

#[async_trait]
impl UTransport for UPClientMqtt {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        // validate message
        let attributes = message.attributes.as_ref().ok_or(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Unable to parse uAttributes",
        ))?;

        // validate uattributes content
        let validator = UAttributesValidators::get_validator_for_attributes(attributes);
        validator.validate(attributes).map_err(|e| {
            UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                format!("Invalid uAttributes, err: {e:?}"),
            )
        })?;

        // Get mqtt topic string from source and sink uuris
        let src_uri = attributes.source.as_ref().ok_or(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Invalid source: expected a source value, none was found",
        ))?;
        let sink_uri = attributes.sink.as_ref();
        let topic = self.to_mqtt_topic_string(src_uri, sink_uri);

        // Extract payload from umessage to send
        let payload = message.payload.clone();

        self.send_message(&topic, attributes, payload).await
    }

    async fn register_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        let topic = self.to_mqtt_topic_string(source_filter, sink_filter);

        self.add_listener(&topic, listener).await
    }

    async fn unregister_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        let topic: String = self.to_mqtt_topic_string(source_filter, sink_filter);

        self.remove_listener(&topic, listener).await
    }

    async fn receive(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
    ) -> Result<UMessage, UStatus> {
        Err(UStatus::fail_with_code(
            UCode::UNIMPLEMENTED,
            "This method is not implemented for mqtt. Use register_listener instead.",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        str::FromStr,
    };

    use async_std::sync::RwLock;
    use paho_mqtt::{self as mqtt, MQTT_VERSION_5, QOS_1};
    use protobuf::Enum;
    use up_rust::{
        ComparableListener, UAttributes, UListener, UMessageType, UPayloadFormat, UPriority,
        UUIDBuilder, UUID,
    };

    use test_case::test_case;

    use crate::{MockableMqttClient, MqttConfig};

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

    pub struct MockMqttClient {}

    #[async_trait]
    impl MockableMqttClient for MockMqttClient {
        async fn new_client(
            _config: MqttConfig,
            _client_id: UUID,
            _on_receive: fn(
                Option<paho_mqtt::Message>,
                Arc<RwLock<HashMap<String, HashSet<ComparableListener>>>>,
            ),
            _topic_map: Arc<RwLock<HashMap<String, HashSet<ComparableListener>>>>,
        ) -> Result<Self, UStatus>
        where
            Self: Sized,
        {
            Ok(Self {})
        }

        async fn publish(&self, _mqtt_message: mqtt::Message) -> Result<(), UStatus> {
            Ok(())
        }

        async fn subscribe(&self, _topic: &str) -> Result<(), UStatus> {
            Ok(())
        }

        async fn unsubscribe(&self, _topic: &str) -> Result<(), UStatus> {
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
}
