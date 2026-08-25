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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.nats.client.Connection;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class NatsConnectionWatchdogTest {

    private final NatsConnectionFactory factory = mock(NatsConnectionFactory.class);
    private final NatsStreamManager streamManager = mock(NatsStreamManager.class);
    private final NatsConnectionWatchdog watchdog =
            new NatsConnectionWatchdog(factory, streamManager);

    @Test
    void checkConnection_skipsRebuildWhenConnectionIsConnected() throws Exception {
        Connection connection = mock(Connection.class);
        when(factory.getCachedConnection()).thenReturn(connection);
        when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);

        watchdog.checkConnection();

        verify(factory, never()).createConnectionIfNeeded();
        verifyNoInteractions(streamManager);
    }

    @Test
    void checkConnection_skipsRebuildWhileClientIsReconnecting() throws Exception {
        Connection connection = mock(Connection.class);
        when(factory.getCachedConnection()).thenReturn(connection);
        when(connection.getStatus()).thenReturn(Connection.Status.RECONNECTING);

        watchdog.checkConnection();

        verify(factory, never()).createConnectionIfNeeded();
        verifyNoInteractions(streamManager);
    }

    @Test
    void checkConnection_repairsResourcesAfterInPlaceReconnect() throws Exception {
        Connection connection = mock(Connection.class);
        when(factory.getCachedConnection()).thenReturn(connection);
        when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);
        when(factory.isResourceRepairRequired()).thenReturn(true);
        when(factory.getResourceRepairGeneration()).thenReturn(2L);

        watchdog.checkConnection();

        verify(factory, never()).createConnectionIfNeeded();
        verify(streamManager).reconcileNatsResourcesStrict();
        verify(factory).markResourceRepairComplete(2L);
    }

    @Test
    void checkConnection_rebuildsAndRepairsResourcesWhenConnectionIsNull() throws Exception {
        when(factory.getCachedConnection()).thenReturn(null);
        when(factory.getResourceRepairGeneration()).thenReturn(1L);

        watchdog.checkConnection();

        verify(factory).requireResourceRepair();
        verify(factory).createConnectionIfNeeded();
        verify(streamManager).reconcileNatsResourcesStrict();
        verify(factory).markResourceRepairComplete(1L);
    }

    @Test
    void checkConnection_rebuildsWhenConnectionIsClosed() throws Exception {
        Connection connection = mock(Connection.class);
        when(factory.getCachedConnection()).thenReturn(connection);
        when(connection.getStatus()).thenReturn(Connection.Status.CLOSED);
        when(factory.getResourceRepairGeneration()).thenReturn(1L);

        watchdog.checkConnection();

        verify(factory).requireResourceRepair();
        verify(factory).createConnectionIfNeeded();
        verify(streamManager).reconcileNatsResourcesStrict();
        verify(factory).markResourceRepairComplete(1L);
    }

    @Test
    void checkConnection_retriesResourceRepairAfterFailure() throws Exception {
        Connection connection = mock(Connection.class);
        when(factory.getCachedConnection()).thenReturn(connection);
        when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);
        when(factory.isResourceRepairRequired()).thenReturn(true);
        when(factory.getResourceRepairGeneration()).thenReturn(2L);
        doThrow(new IOException("Stream creation failed"))
                .when(streamManager).reconcileNatsResourcesStrict();

        watchdog.checkConnection();
        watchdog.checkConnection();

        verify(streamManager, times(2)).reconcileNatsResourcesStrict();
        verify(factory, never()).markResourceRepairComplete(2L);
    }

    @Test
    void checkConnection_toleratesReconnectFailure() throws Exception {
        when(factory.getCachedConnection()).thenReturn(null);
        doThrow(new IOException("Connection refused")).when(factory).createConnectionIfNeeded();

        assertDoesNotThrow(watchdog::checkConnection);

        verify(streamManager, never()).reconcileNatsResourcesStrict();
        verify(factory, never()).markResourceRepairComplete(anyLong());
    }
}
