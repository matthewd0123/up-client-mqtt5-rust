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

use paho_mqtt::{self as mqtt, MQTT_VERSION_5};
use protobuf::Message;
use serde::{Deserialize, Serialize};
use up_rust::uprotocol::{UAttributes, UCode, UMessage, UPayload, UStatus};

pub mod transport;

pub type UListener = Box<dyn Fn(Result<UMessage, UStatus>) + Send + Sync + 'static>;

const UPAYLOAD_VERSION: u8 = 1;
const UATTRIBUTE_VERSION: u8 = 1;

pub struct MqttCliConfig {
    pub mqtt_port: String,
    pub mqtt_namespace: String,
}

#[derive(Serialize, Deserialize)]
pub struct UMessageBytes {
    pub payload_bytes: UPayloadBytes,
    pub attributes_bytes: UAttributesBytes,
}

#[derive(Serialize, Deserialize)]
pub struct UPayloadBytes {
    pub version: u8,
    pub payload: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct UAttributesBytes {
    pub version: u8,
    pub attributes: Vec<u8>,
}

pub struct UPClientMqtt {
    pub mqtt_cli: Arc<mqtt::AsyncClient>,
}

impl UPClientMqtt {
    pub async fn new(config: MqttCliConfig) -> Result<Self, UStatus> {
        let mqtt_uri = format!("mqtt://{}:{}", config.mqtt_namespace, config.mqtt_port);

        let mqtt_cli = mqtt::CreateOptionsBuilder::new()
            .server_uri(mqtt_uri)
            .mqtt_version(MQTT_VERSION_5)
            .create_client()
            .map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Unable to create mqtt client: {e:?}"),
                )
            })?;

        let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
            .clean_session(true)
            .finalize();

        mqtt_cli.connect(conn_opts).await.map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Unable to connect to mqtt broker: {e:?}"),
            )
        })?;

        Ok(Self {
            mqtt_cli: Arc::new(mqtt_cli),
        })
    }

    fn uattributes_to_bytes(attributes: &UAttributes) -> Result<UAttributesBytes, UStatus> {
        let bytes = &attributes.write_to_bytes().map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Unable to serialize uAttributes: {e:?}"),
            )
        })?;

        Ok(UAttributesBytes {
            version: UATTRIBUTE_VERSION,
            attributes: bytes.to_vec(),
        })
    }

    fn bytes_to_uattributes(bytes: &UAttributesBytes) -> Result<UAttributes, UStatus> {
        if bytes.version != UATTRIBUTE_VERSION {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid uAttributes version",
            ));
        }

        let attributes = Message::parse_from_bytes(&bytes.attributes).map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Unable to deserialize uAttributes: {e:?}"),
            )
        })?;

        Ok(attributes)
    }

    fn upayload_to_bytes(payload: &UPayload) -> Result<UPayloadBytes, UStatus> {
        let bytes = &payload.write_to_bytes().map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Unable to serialize uPayload: {e:?}"),
            )
        })?;

        Ok(UPayloadBytes {
            version: UPAYLOAD_VERSION,
            payload: bytes.to_vec(),
        })
    }

    fn deserialize_upayload(bytes: &UPayloadBytes) -> Result<UPayload, UStatus> {
        if bytes.version != UPAYLOAD_VERSION {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid uPayload version",
            ));
        }

        let payload = Message::parse_from_bytes(&bytes.payload).map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Unable to deserialize uPayload: {e:?}"),
            )
        })?;

        Ok(payload)
    }
}
