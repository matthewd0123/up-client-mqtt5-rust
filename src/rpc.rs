/********************************************************************************
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
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

use crate::UPClientMqtt;
use async_std::future::timeout;
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use up_rust::{
    RpcClient, RpcClientResult, UAttributesValidators, UListener, UMessage, UMessageError, UStatus,
    UUri,
};

struct RpcListener {
    sender: mpsc::Sender<RpcClientResult>,
    correlation_id: String,
}

impl RpcListener {
    pub fn new(sender: mpsc::Sender<RpcClientResult>, correlation_id: String) -> Self {
        Self {
            sender,
            correlation_id,
        }
    }
}

#[async_trait]
impl UListener for RpcListener {
    // TODO: Handle send errors
    async fn on_error(&self, status: UStatus) {
        println!("Error: {:?}", status);

        let _ = self
            .sender
            .send(RpcClientResult::Err(UMessageError::PayloadError(
                status.get_message(),
            )))
            .await;
    }

    // TODO: Handle send and unwrap errors
    async fn on_receive(&self, message: UMessage) {
        println!("Received message: {:?}", message);

        if message
            .attributes
            .as_ref()
            .unwrap()
            .reqid
            .as_ref()
            .unwrap()
            .to_hyphenated_string()
            != self.correlation_id
        {
            // Ignore message
            return;
        }

        let _ = self.sender.send(RpcClientResult::Ok(message)).await;
    }
}

#[async_trait]
impl RpcClient for UPClientMqtt {
    async fn invoke_method(&self, _method: UUri, request: UMessage) -> RpcClientResult {
        let attributes = request.attributes.clone().unwrap();

        // Validate uattributes is an RPC request
        UAttributesValidators::Request
            .validator()
            .validate(&attributes)?;

        // Sink is required for RPC, and this should be caught by the UAttributesValidator.
        let sink = attributes
            .sink
            .as_ref()
            .ok_or(UMessageError::PayloadError("Sink not found".to_string()))?;

        // Request id is required for RPC, and this should be caught by the UAttributesValidator.
        let req_id = attributes
            .reqid
            .as_ref()
            .ok_or(UMessageError::PayloadError(
                "Request ID not found".to_string(),
            ))?;

        // Get MQTT request topic from UAttributes
        let request_topic = self.to_mqtt_topic_string(&attributes.source, Some(&sink));

        // Get MQTT response topic from UAttributes
        let response_topic = self.to_mqtt_topic_string(&sink, Some(&attributes.source));

        // Create a channel to receive RPC response from listener
        let (sender, mut receiver) = mpsc::channel(100);

        let listener = RpcListener::new(sender, req_id.to_hyphenated_string());

        self.add_listener(&response_topic, Arc::new(listener))
            .await
            .map_err(|err| UMessageError::PayloadError(err.get_message()))?;

        // Send RPC request
        self.send_message(&request_topic, &request, &attributes)
            .await
            .map_err(|err| UMessageError::PayloadError(err.get_message()))?;

        // Recieve RPC response, error on ttl.
        let rpc_response = if let Some(ttl) = attributes.ttl {
            timeout(Duration::from_millis(u64::from(ttl)), receiver.recv()).await
        } else {
            Ok(receiver.recv().await)
        }
        .map_err(|_| UMessageError::PayloadError("RPC response timeout".to_string()))?
        .ok_or_else(|| UMessageError::PayloadError("RPC response error".to_string()))?;

        rpc_response
    }
}
