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
    fs::File,
    io::BufReader,
    path::Path,
    str::{self, FromStr},
    sync::Arc,
    time::SystemTime,
};

use async_trait::async_trait;
use env_logger::{Builder, Target};
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use up_client_mqtt5_rust::{MqttConfig, UPClientMqtt, UPClientMqttType};
use up_rust::{
    UCode, UListener, UMessage, UMessageBuilder, UPayloadFormat, UStatus, UTransport, UUri, UUID,
};

fn read_user_from_file<P: AsRef<Path>>(path: P) -> ClientConfigOptions {
    // Open the file in read-only mode with buffer.
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);

    serde_json::from_reader(reader).unwrap()
}

#[derive(Debug, Serialize, Deserialize)]
struct ClientConfigOptions {
    authority_name: Option<String>,
    send_topics: Vec<(String, String, String)>,
    recv_topics: Vec<(String, String)>,
}

struct PrintlnListener {}

#[async_trait]
impl UListener for PrintlnListener {
    async fn on_receive(&self, message: UMessage) {
        let msg_payload = message.clone().payload.unwrap();
        let msg_str: &str = str::from_utf8(&msg_payload).unwrap();
        println!("Received message: {msg_str}");
        println!("Message: {message}");
    }
}

#[tokio::main]
async fn main() -> Result<(), UStatus> {
    Builder::new()
        .filter(None, LevelFilter::Warn)
        .target(Target::Stdout)
        .init();

    let mut config_file = "./examples/config/client_config.json";
    let args = std::env::args().collect::<Vec<String>>();

    if args.len() > 2 {
        println!("Usage: {} [config_file]", args[0]);
        return Ok(());
    }

    if args.len() == 2 {
        config_file = &args[1];
    }

    let cli_options = read_user_from_file(config_file);

    let config = MqttConfig {
        mqtt_hostname: "localhost".to_string(),
        mqtt_port: "8883".to_string(),
        max_buffered_messages: 100,
        max_subscriptions: 100,
        session_expiry_interval: 3600,
        ssl_options: None,
    };

    let client = UPClientMqtt::new(
        config,
        UUID::build(),
        cli_options
            .authority_name
            .unwrap_or("Vehicle_1".to_string()),
        UPClientMqttType::Device,
    )
    .await?;

    for (src, sink) in cli_options.recv_topics.iter() {
        let source_filter = UUri::from_str(src).expect("Failed to create source filter");
        let sink_filter = if sink.is_empty() {
            None
        } else {
            let sink_uri = UUri::from_str(sink).map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Failed to create sink from config entry: {e}"),
                )
            })?;
            Some(sink_uri)
        };
        let listener = Arc::new(PrintlnListener {});

        println!(
            "Subscribing to source filter: {} and sink filter: {}",
            source_filter.to_uri(false),
            sink_filter
                .clone()
                .map(|u| u.to_uri(false))
                .unwrap_or("none".to_string())
        );

        client
            .register_listener(&source_filter, sink_filter.as_ref(), listener)
            .await?;
    }

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let current_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        for (src, sink, msg_type) in cli_options.send_topics.iter() {
            let source_uri = UUri::from_str(src).map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Failed to create source from config entry: {e}"),
                )
            })?;
            let sink_uri = if sink.is_empty() {
                None
            } else {
                let uuri = UUri::from_str(sink).map_err(|e| {
                    UStatus::fail_with_code(
                        UCode::INTERNAL,
                        format!("Failed to create sink from config entry: {e}"),
                    )
                })?;
                Some(uuri)
            };

            if msg_type.is_empty() {
                return Err(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Message type is required",
                ));
            }

            let message = if msg_type.eq_ignore_ascii_case("publish") {
                UMessageBuilder::publish(source_uri.clone())
                    .build_with_payload(
                        current_time.to_string(),
                        UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
                    )
                    .expect("Failed to build message")
            } else if msg_type.eq_ignore_ascii_case("notification") {
                UMessageBuilder::notification(source_uri.clone(), sink_uri.clone().unwrap())
                    .build_with_payload(
                        current_time.to_string(),
                        UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
                    )
                    .expect("Failed to build message")
            } else {
                return Err(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "message type not supported",
                ));
            };

            println!(
                "Sending message: {} to source: {} and sink: {}",
                current_time,
                source_uri.to_uri(false),
                sink_uri
                    .map(|u| u.to_uri(false))
                    .unwrap_or("none".to_string())
            );

            client.send(message).await?;
        }
    }
}
