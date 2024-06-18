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
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, format!("Invalid uAttributes, err: {e:?}"))
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
