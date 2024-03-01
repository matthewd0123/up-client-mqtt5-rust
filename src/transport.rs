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

use async_trait::async_trait;

use paho_mqtt::{self as mqtt};

use up_rust::{
    transport::{datamodel::UTransport, validator::Validators},
    uprotocol::{UAttributes, UCode, UMessage, UMessageType, UPayload, UStatus, UUri},
    uuid::builder::UUIDBuilder,
};

use crate::{UMessageBytes, UPClientMqtt};

impl UPClientMqtt {
    async fn send_msg(
        &self,
        topic: &str,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        let payload_bytes = UPClientMqtt::upayload_to_bytes(&payload)?;
        let attributes_bytes = UPClientMqtt::uattributes_to_bytes(&attributes)?;

        let msg_data = UMessageBytes {
            payload_bytes,
            attributes_bytes,
        };

        let msg = mqtt::MessageBuilder::new()
            .topic(topic)
            .payload(serde_json::to_vec(&msg_data).map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Unable to serialize payload: {e:?}"),
                )
            })?)
            .qos(1)
            .finalize();

        self.mqtt_cli.publish(msg).await.map_err(|e| {
            UStatus::fail_with_code(UCode::INTERNAL, format!("Unable to publish message: {e:?}"))
        })?;

        Ok(())
    }
}

#[async_trait]
impl UTransport for UPClientMqtt {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        // validate message
        let attributes = *message.attributes.0.ok_or(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Invalid uAttributes",
        ))?;

        let payload = *message.payload.0.ok_or(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Invalid uPayload",
        ))?;

        // Match UAttributes type (Publish / Request / Response / Unspecified)
        let topic = match attributes
            .type_
            .enum_value()
            .map_err(|_| UStatus::fail_with_code(UCode::INTERNAL, "Unable to parse type"))?
        {
            UMessageType::UMESSAGE_TYPE_PUBLISH => {
                Validators::Publish
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        UStatus::fail_with_code(
                            UCode::INVALID_ARGUMENT,
                            format!("Wrong Publish UAttributes {e:?}"),
                        )
                    })?;

                attributes.clone().source
            }
            UMessageType::UMESSAGE_TYPE_REQUEST => {
                Validators::Request
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        UStatus::fail_with_code(
                            UCode::INVALID_ARGUMENT,
                            format!("Wrong Request UAttributes {e:?}"),
                        )
                    })?;

                attributes.clone().sink
            }
            UMessageType::UMESSAGE_TYPE_RESPONSE => {
                Validators::Response
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        UStatus::fail_with_code(
                            UCode::INVALID_ARGUMENT,
                            format!("Wrong Response UAttributes {e:?}"),
                        )
                    })?;

                attributes.clone().sink
            }
            UMessageType::UMESSAGE_TYPE_UNSPECIFIED => {
                return Err(UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    "Wrong Message type in UAttributes",
                ))
            }
        };

        // TODO: Does the UURI need to be transformed/encoded?
        let topic_str = topic.to_string();

        self.send_msg(&topic_str, payload, attributes).await
    }

    async fn register_listener(
        &self,
        topic: UUri,
        listener: Box<dyn Fn(Result<UMessage, UStatus>) + Send + Sync + 'static>,
    ) -> Result<String, UStatus> {
        // implementation goes here
        println!("Registering listener for topic: {:?}", topic);

        listener(Ok(UMessage::new()));

        let listener_id = UUIDBuilder::new().build().to_string();

        Ok(listener_id)
    }

    async fn unregister_listener(&self, topic: UUri, listener: &str) -> Result<(), UStatus> {
        // implementation goes here
        println!("Unregistering listener: {listener} for topic: {:?}", topic);

        Ok(())
    }

    async fn receive(&self, _topic: UUri) -> Result<UMessage, UStatus> {
        Err(UStatus::fail_with_code(
            UCode::UNIMPLEMENTED,
            "This method is not implemented for mqtt. Use register_listener instead.",
        ))
    }
}
