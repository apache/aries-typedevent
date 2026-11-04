/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.aries.typedevent.bus.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.aries.typedevent.bus.spi.CustomEventConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.osgi.service.typedevent.monitor.MonitorEvent;
import org.osgi.service.typedevent.monitor.RangePolicy;
import org.osgi.util.pushstream.PushStream;

/**
 * The monitor only needs the untyped form of an event if the event will be
 * stored in the history or sent to a connected stream. Flattening is the most
 * expensive step of delivery, so it must be skipped otherwise.
 */
public class MonitorEventTaskTest {

    private static final String TOPIC = "a/b/c";

    private TypedEventMonitorImpl monitorImpl;

    private TypedEventMonitorImpl monitor(boolean historyEnabledAtStart) {
        monitorImpl = new TypedEventMonitorImpl(Map.of("event.history.enable.at.start", String.valueOf(historyEnabledAtStart)));
        return monitorImpl;
    }

    @AfterEach
    public void stop() {
        if (monitorImpl != null) {
            monitorImpl.destroy();
        }
    }

    @Test
    public void testHistoryIsEnabledByDefault() throws Exception {
        monitorImpl = new TypedEventMonitorImpl(Map.of());

        assertTrue(monitorImpl.isMonitored(TOPIC));
    }

    @Test
    public void testNotMonitoredWithoutHistoryOrStream() throws Exception {
        assertFalse(monitor(false).isMonitored(TOPIC));
    }

    @Test
    public void testHistoryPolicyOnlyMonitorsMatchingTopics() throws Exception {
        monitor(false).configureHistoryStorage("a/*", RangePolicy.atMost(10));

        assertTrue(monitorImpl.isMonitored(TOPIC));
        assertFalse(monitorImpl.isMonitored("x/y/z"));
    }

    @Test
    public void testHistoryPolicyWithNoStorageIsNotMonitored() throws Exception {
        monitor(true).configureHistoryStorage("*", RangePolicy.none());

        assertFalse(monitorImpl.isMonitored(TOPIC));
    }

    @Test
    public void testConnectedStreamMonitorsEveryTopic() throws Exception {
        monitor(false);

        PushStream<MonitorEvent> stream = monitorImpl.monitorEvents();
        try {
            stream.forEach(e -> {
            });

            // The stream connects to the event source asynchronously
            long deadline = System.nanoTime() + 5_000_000_000L;
            while (!monitorImpl.isMonitored(TOPIC) && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertTrue(monitorImpl.isMonitored(TOPIC));
            assertTrue(monitorImpl.isMonitored("x/y/z"));
        } finally {
            stream.close();
        }

        long deadline = System.nanoTime() + 5_000_000_000L;
        while (monitorImpl.isMonitored(TOPIC) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertFalse(monitorImpl.isMonitored(TOPIC));
    }

    @Test
    public void testEventIsNotFlattenedWhenNotMonitored() {
        Object event = new Object();
        CustomEventConverter custom = Mockito.mock(CustomEventConverter.class);
        EventConverter converter = EventConverter.forTypedEvent(event, custom);

        new MonitorEventTask(TOPIC, converter, monitor(false)).notifyListener();

        Mockito.verifyNoInteractions(custom);
    }

    @Test
    public void testEventIsFlattenedAndRecordedWhenMonitored() throws Exception {
        Object event = new Object();
        CustomEventConverter custom = Mockito.mock(CustomEventConverter.class);
        Mockito.when(custom.toUntypedEvent(event)).thenReturn(Map.of("message", "boo"));
        EventConverter converter = EventConverter.forTypedEvent(event, custom);

        new MonitorEventTask(TOPIC, converter, monitor(true)).notifyListener();

        List<MonitorEvent> history = monitorImpl.monitorEvents(10, true).collect(Collectors.toList()).getValue();
        assertEquals(1, history.size());
        assertEquals(TOPIC, history.get(0).topic);
        assertEquals("boo", history.get(0).eventData.get("message"));
    }
}
