/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.icms.outbound.nats;

import io.nats.client.Connection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically repairs a terminally closed NATS connection so that losing the connection
 * (for example when a NATS server restart ends in a reconnect authentication failure,
 * which jnats treats as fatal even with unlimited reconnects) does not wedge ICMS until the
 * next inbound publish or a manual pod restart.
 *
 * <p>After a successful rebuild or in-place reconnect it reconciles every configured
 * JetStream stream and consumer. Failed reconciliation remains pending and is retried.
 */
@Component
@ConditionalOnProperty(prefix = "icms.nats", name = "nats-enabled", havingValue = "true")
@Slf4j
public class NatsConnectionWatchdog {

    private final NatsConnectionFactory natsConnectionFactory;
    private final NatsStreamManager natsStreamManager;

    public NatsConnectionWatchdog(
            NatsConnectionFactory natsConnectionFactory,
            NatsStreamManager natsStreamManager) {
        this.natsConnectionFactory = natsConnectionFactory;
        this.natsStreamManager = natsStreamManager;
    }

    @Scheduled(initialDelayString = "${icms.nats.connection-watchdog-initial-delay:PT1M}",
               fixedDelayString = "${icms.nats.connection-watchdog-interval:PT30S}")
    public void checkConnection() {
        Connection connection = natsConnectionFactory.getCachedConnection();
        Connection.Status status = connection == null ? null : connection.getStatus();
        if (connection != null
                && status != Connection.Status.CONNECTED
                && status != Connection.Status.CLOSED) {
            return;
        }
        boolean rebuildRequired = connection == null || status == Connection.Status.CLOSED;
        if (!rebuildRequired && !natsConnectionFactory.isResourceRepairRequired()) {
            return;
        }

        try {
            if (rebuildRequired) {
                natsConnectionFactory.requireResourceRepair();
                natsConnectionFactory.createConnectionIfNeeded();
            }
            long repairGeneration = natsConnectionFactory.getResourceRepairGeneration();
            natsStreamManager.reconcileNatsResourcesStrict();
            natsConnectionFactory.markResourceRepairComplete(repairGeneration);
            log.info("NATS connection and resources restored by watchdog");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("NATS connection watchdog interrupted while restoring the connection", e);
        } catch (Exception e) {
            log.warn("NATS connection watchdog failed to restore the connection and resources", e);
        }
    }
}
