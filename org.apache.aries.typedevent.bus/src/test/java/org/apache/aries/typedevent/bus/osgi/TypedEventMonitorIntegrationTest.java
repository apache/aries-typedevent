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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or eventBusied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aries.typedevent.bus.osgi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.aries.typedevent.bus.common.TestEvent;
import org.apache.aries.typedevent.bus.common.TestEventConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.service.typedevent.TypedEventBus;
import org.osgi.service.typedevent.TypedEventHandler;
import org.osgi.service.typedevent.monitor.MonitorEvent;
import org.osgi.service.typedevent.monitor.TypedEventMonitor;
import org.osgi.test.common.annotation.InjectBundleContext;
import org.osgi.test.common.annotation.InjectService;
import org.osgi.test.junit5.context.BundleContextExtension;
import org.osgi.test.junit5.service.ServiceExtension;
import org.osgi.util.promise.Promise;


@ExtendWith(BundleContextExtension.class)
@ExtendWith(ServiceExtension.class)
public class TypedEventMonitorIntegrationTest extends AbstractIntegrationTest {

    TypedEventMonitor monitor;
    
    TypedEventBus eventBus;
    
    @Mock
    TestEventConsumer typedEventHandler;

    private AutoCloseable mocks;

    private static Bundle eventBusBundle;
    
    @BeforeAll
    public static void clearInitialHistory(@InjectBundleContext BundleContext ctx) throws Exception {
        eventBusBundle = getBusBundle(ctx);
    
        eventBusBundle.stop();
        eventBusBundle.start();
    }
    
    private static Bundle getBusBundle(BundleContext ctx) {
        return Arrays.stream(ctx.getBundles())
                .filter(b -> "org.apache.aries.typedevent.bus".equals(b.getSymbolicName()))
                .findAny().orElse(null);
    }

    @BeforeEach
    public void setupMocks() {
        mocks = MockitoAnnotations.openMocks(this);
    }

    /**
     * Inject services every time as we restart the eventBus after each test
     * @param monitor
     * @param bus
     */
    @BeforeEach
    public void setupMocks(@InjectService TypedEventMonitor monitor, @InjectService TypedEventBus bus) {
        this.monitor = monitor;
        this.eventBus = bus;
    }
    
    @AfterEach
    public void stop() throws Exception {
        mocks.close();

        // Needed to clear history from previous tests
        eventBusBundle.stop();
        eventBusBundle.start();
    }

    /**
     * Tests that events are delivered to the monitor
     *
     * @throws InterruptedException
     * @throws InvocationTargetException
     */
    @Test
    public void testTypedEventMonitor1() throws InterruptedException, InvocationTargetException {

        Promise<List<MonitorEvent>> eventsPromise = monitor.monitorEvents()
                .limit(2)
                .collect(Collectors.toList());

        TestEvent event = new TestEvent();
        event.message = "boo";

        Dictionary<String, Object> props = new Hashtable<>();

        regs.add(eventBusBundle.getBundleContext().registerService(TypedEventHandler.class, typedEventHandler, props));

        eventBus.deliver(event);

        event = new TestEvent();
        event.message = "bam";

        eventBus.deliver(event);


        Mockito.verify(typedEventHandler, Mockito.timeout(2000)).notify(
                Mockito.eq(TEST_EVENT_TOPIC), Mockito.argThat(isTestEventWithMessage("boo")));
        Mockito.verify(typedEventHandler, Mockito.timeout(2000)).notify(
                Mockito.eq(TEST_EVENT_TOPIC), Mockito.argThat(isTestEventWithMessage("bam")));

        List<MonitorEvent> events = eventsPromise.timeout(100).getValue();

        assertEquals(2, events.size());

        assertEquals(TEST_EVENT_TOPIC, events.get(0).topic);
        assertEquals(TEST_EVENT_TOPIC, events.get(1).topic);

        assertEquals("boo", events.get(0).eventData.get("message"));
        assertEquals("bam", events.get(1).eventData.get("message"));


    }


    /**
     * Tests that events are delivered to the monitor even when nobody is listening
     *
     * @throws InterruptedException
     * @throws InvocationTargetException
     */
    @Test
    public void testTypedEventMonitor2() throws InterruptedException, InvocationTargetException {

        Promise<List<MonitorEvent>> eventsPromise = monitor.monitorEvents()
                .limit(2)
                .collect(Collectors.toList());

        TestEvent event = new TestEvent();
        event.message = "boo";

        eventBus.deliver(event);

        event = new TestEvent();
        event.message = "bam";

        eventBus.deliver(event);

        List<MonitorEvent> events = eventsPromise.timeout(2000).getValue();

        assertEquals(2, events.size());

        assertEquals(TEST_EVENT_TOPIC, events.get(0).topic);
        assertEquals(TEST_EVENT_TOPIC, events.get(1).topic);

        assertEquals("boo", events.get(0).eventData.get("message"));
        assertEquals("bam", events.get(1).eventData.get("message"));


    }

    /**
     * Tests that event history is delivered to the monitor
     *
     * @throws InterruptedException
     * @throws InvocationTargetException
     */
    @Test
    public void testTypedEventMonitorHistory1() throws InterruptedException, InvocationTargetException {

        TestEvent event = new TestEvent();
        event.message = "boo";

        eventBus.deliver(event);

        event = new TestEvent();
        event.message = "bam";

        eventBus.deliver(event);

        Thread.sleep(500);

        Promise<List<MonitorEvent>> eventsPromise = monitor.monitorEvents()
                .limit(Duration.ofSeconds(1))
                .collect(Collectors.toList())
                .timeout(2000);

        List<MonitorEvent> events = eventsPromise.getValue();

        assertTrue(events.isEmpty());

        eventsPromise = monitor.monitorEvents(5)
                .limit(Duration.ofSeconds(1))
                .collect(Collectors.toList())
                .timeout(2000);

        events = eventsPromise.getValue();

        assertEquals(2, events.size(), events.toString());

        assertEquals(TEST_EVENT_TOPIC, events.get(0).topic);
        assertEquals(TEST_EVENT_TOPIC, events.get(1).topic);

        assertEquals("boo", events.get(0).eventData.get("message"));
        assertEquals("bam", events.get(1).eventData.get("message"));

        eventsPromise = monitor.monitorEvents(1)
                .limit(Duration.ofSeconds(1))
                .collect(Collectors.toList())
                .timeout(2000);

        events = eventsPromise.getValue();

        assertEquals(1, events.size());

        assertEquals(TEST_EVENT_TOPIC, events.get(0).topic);

        assertEquals("bam", events.get(0).eventData.get("message"));


    }

    /**
     * Tests that event history is delivered to the monitor
     *
     * @throws InterruptedException
     * @throws InvocationTargetException
     */
    @Test
    public void testTypedEventMonitorHistory2() throws InterruptedException, InvocationTargetException {

        Instant beforeFirst = Instant.now().minus(Duration.ofMillis(500));

        TestEvent event = new TestEvent();
        event.message = "boo";

        eventBus.deliver(event);

        Instant afterFirst = Instant.now().plus(Duration.ofMillis(500));

        Thread.sleep(1000);

        event = new TestEvent();
        event.message = "bam";

        eventBus.deliver(event);

        Instant afterSecond = Instant.now().plus(Duration.ofMillis(500));

        Thread.sleep(500);

        Promise<List<MonitorEvent>> eventsPromise = monitor.monitorEvents()
                .limit(Duration.ofSeconds(1))
                .collect(Collectors.toList())
                .timeout(2000);


        List<MonitorEvent> events = eventsPromise.getValue();

        assertTrue(events.isEmpty());

        eventsPromise = monitor.monitorEvents(beforeFirst)
                .limit(Duration.ofSeconds(1))
                .collect(Collectors.toList())
                .timeout(2000);

        events = eventsPromise.getValue();

        assertEquals(2, events.size());

        assertEquals(TEST_EVENT_TOPIC, events.get(0).topic);
        assertEquals(TEST_EVENT_TOPIC, events.get(1).topic);

        assertEquals("boo", events.get(0).eventData.get("message"));
        assertEquals("bam", events.get(1).eventData.get("message"));

        eventsPromise = monitor.monitorEvents(afterFirst)
                .limit(Duration.ofSeconds(1))
                .collect(Collectors.toList())
                .timeout(2000);

        events = eventsPromise.getValue();

        assertEquals(1, events.size());

        assertEquals(TEST_EVENT_TOPIC, events.get(0).topic);

        assertEquals("bam", events.get(0).eventData.get("message"));

        eventsPromise = monitor.monitorEvents(afterSecond)
                .limit(Duration.ofSeconds(1))
                .collect(Collectors.toList())
                .timeout(2000);

        events = eventsPromise.getValue();

        assertTrue(events.isEmpty());
    }

}
