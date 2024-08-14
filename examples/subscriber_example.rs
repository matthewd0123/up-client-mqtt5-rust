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
    str::{self, FromStr},
    sync::Arc,
};

use async_trait::async_trait;
use up_client_mqtt5_rust::{MqttConfig, UPClientMqtt, UPClientMqttType};
use up_rust::{UListener, UMessage, UStatus, UTransport, UUIDBuilder, UUri, UUID};

const WILDCARD_ENTITY_ID: u32 = 0x0000_FFFF;
const WILDCARD_ENTITY_VERSION: u32 = 0x0000_00FF;
const WILDCARD_RESOURCE_ID: u32 = 0x0000_FFFF;

struct PrintlnListener {}

#[async_trait]
impl UListener for PrintlnListener {
    async fn on_receive(&self, message: UMessage) {
        let msg_payload = message.payload.unwrap();
        let msg_str: &str = str::from_utf8(&msg_payload).unwrap();
        println!("Received message: {msg_str}");
    }

    async fn on_error(&self, err: UStatus) {
        println!("SimpleListener: Encountered an error: {err:?}");
    }
}

#[tokio::main]
async fn main() -> Result<(), UStatus> {
    let config = MqttConfig {
        mqtt_hostname: "localhost".to_string(),
        mqtt_port: "1883".to_string(),
        max_buffered_messages: 100,
        max_subscriptions: 100,
        session_expiry_interval: 3600,
        ssl_options: None,
    };

    let client = UPClientMqtt::new(
        config,
        UUIDBuilder::build(),
        "Vehicle_A".to_string(),
        UPClientMqttType::Device,
    )
    .await?;

    let listener = Arc::new(PrintlnListener {});
    let source_filter = UUri::from_str(&format!(
        "//Vehicle_B/{WILDCARD_ENTITY_ID:X}/{WILDCARD_ENTITY_VERSION:X}/{WILDCARD_RESOURCE_ID:X}"
    ))
    .expect("Failed to create source filter");

    println!("Subscribing to: {}", source_filter.to_uri(false));

    client
        .register_listener(&source_filter, None, listener.clone())
        .await?;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
