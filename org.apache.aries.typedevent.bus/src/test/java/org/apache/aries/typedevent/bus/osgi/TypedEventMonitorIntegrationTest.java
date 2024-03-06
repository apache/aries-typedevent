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

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.aries.typedevent.bus.common.TestEvent;
import org.apache.aries.typedevent.bus.common.TestEvent2;
import org.apache.aries.typedevent.bus.common.TestEvent2.EventType;
import org.apache.aries.typedevent.bus.common.TestEventConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.service.typedevent.TypedEventBus;
import org.osgi.service.typedevent.TypedEventHandler;
import org.osgi.service.typedevent.monitor.MonitorEvent;
import org.osgi.service.typedevent.monitor.TypedEventMonitor;
import org.osgi.test.common.annotation.InjectBundleContext;
import org.osgi.test.common.annotation.InjectService;
import org.osgi.test.common.annotation.Property;
import org.osgi.test.common.annotation.Property.Scalar;
import org.osgi.test.common.annotation.config.WithConfiguration;
import org.osgi.test.junit5.cm.ConfigurationExtension;
import org.osgi.test.junit5.context.BundleContextExtension;
import org.osgi.test.junit5.service.ServiceExtension;
import org.osgi.util.promise.Promise;


@ExtendWith(BundleContextExtension.class)
@ExtendWith(ServiceExtension.class)
@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class TypedEventMonitorIntegrationTest extends AbstractIntegrationTest {

    @Mock
    TestEventConsumer typedEventHandler;

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

    @AfterEach
    public void stop() throws Exception {
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
    public void testTypedEventMonitor1(@InjectService TypedEventMonitor monitor, 
    		@InjectService TypedEventBus eventBus) throws InterruptedException, InvocationTargetException {

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
    public void testTypedEventMonitor2(@InjectService TypedEventMonitor monitor, 
    		@InjectService TypedEventBus eventBus) throws InterruptedException, InvocationTargetException {

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
    public void testTypedEventMonitorHistory1(@InjectService TypedEventMonitor monitor, 
    		@InjectService TypedEventBus eventBus) throws InterruptedException, InvocationTargetException {

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
     * Tests that event history is delivered to the monitor and it
     * closes the stream after
     *
     * @throws InterruptedException
     * @throws InvocationTargetException
     */
    @Test
    public void testTypedEventMonitorHistory1Close(@InjectService TypedEventMonitor monitor, 
    		@InjectService TypedEventBus eventBus) throws InterruptedException, InvocationTargetException {
    	
    	TestEvent event = new TestEvent();
    	event.message = "boo";
    	
    	eventBus.deliver(event);
    	
    	event = new TestEvent();
    	event.message = "bam";
    	
    	eventBus.deliver(event);
    	
    	Thread.sleep(500);
    	
    	Promise<List<MonitorEvent>> eventsPromise = monitor.monitorEvents(5, true)
    			.collect(Collectors.toList())
    			.timeout(2000);
    	
    	List<MonitorEvent> events = eventsPromise.getValue();
    	
    	assertEquals(2, events.size(), events.toString());
    	
    	assertEquals(TEST_EVENT_TOPIC, events.get(0).topic);
    	assertEquals(TEST_EVENT_TOPIC, events.get(1).topic);
    	
    	assertEquals("boo", events.get(0).eventData.get("message"));
    	assertEquals("bam", events.get(1).eventData.get("message"));
    	
    	eventsPromise = monitor.monitorEvents(1, true)
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
    public void testTypedEventMonitorHistory2(@InjectService TypedEventMonitor monitor, 
    		@InjectService TypedEventBus eventBus) throws InterruptedException, InvocationTargetException {

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

    /**
     * Tests that event history is delivered to the monitor and it closes after
     *
     * @throws InterruptedException
     * @throws InvocationTargetException
     */
    @Test
    public void testTypedEventMonitorHistory2Close(@InjectService TypedEventMonitor monitor, 
    		@InjectService TypedEventBus eventBus) throws InterruptedException, InvocationTargetException {
    	
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
    	
    	Thread.sleep(600);
    	
    	// No stream time limit, this stream should auto-close
    	Promise<List<MonitorEvent>> eventsPromise = monitor.monitorEvents(beforeFirst, true)
    			.collect(Collectors.toList())
    			.timeout(2000);
    	
    	List<MonitorEvent> events = eventsPromise.getValue();
    	
    	assertEquals(2, events.size());
    	
    	assertEquals(TEST_EVENT_TOPIC, events.get(0).topic);
    	assertEquals(TEST_EVENT_TOPIC, events.get(1).topic);
    	
    	assertEquals("boo", events.get(0).eventData.get("message"));
    	assertEquals("bam", events.get(1).eventData.get("message"));
    	
    	eventsPromise = monitor.monitorEvents(afterFirst, true)
    			.collect(Collectors.toList())
    			.timeout(2000);
    	
    	events = eventsPromise.getValue();
    	
    	assertEquals(1, events.size());
    	
    	assertEquals(TEST_EVENT_TOPIC, events.get(0).topic);
    	
    	assertEquals("bam", events.get(0).eventData.get("message"));
    	
    	eventsPromise = monitor.monitorEvents(afterSecond, true)
    			.collect(Collectors.toList())
    			.timeout(2000);
    	
    	events = eventsPromise.getValue();
    	
    	assertTrue(events.isEmpty());
    }

    
    @WithConfiguration(pid = "org.apache.aries.typedevent.bus", properties = @Property(key = "event.history.enable.at.start", value = "false", scalar = Scalar.Boolean))
    @Test
    public void testTopicConfig(@InjectService TypedEventMonitor monitor, 
    		@InjectService TypedEventBus eventBus) throws Exception {
    	
    	Entry<Integer, Integer> historyStorage = monitor.getEffectiveHistoryStorage(TEST_EVENT_TOPIC);
    	assertEquals(0, historyStorage.getKey());
    	assertEquals(0, historyStorage.getValue());
    	
    	TestEvent event = new TestEvent();
        event.message = "boo";

        eventBus.deliver(event);

        event = new TestEvent();
        event.message = "bam";

        eventBus.deliver(event);

        Thread.sleep(500);

        Promise<List<MonitorEvent>> eventsPromise = monitor.monitorEvents(5, true)
                .collect(Collectors.toList())
                .timeout(2000);

        List<MonitorEvent> events = eventsPromise.getValue();
        assertTrue(events.isEmpty());
        
        monitor.configureHistoryStorage(TEST_EVENT_TOPIC, 0, 1);
        
        event = new TestEvent();
        event.message = "boo";

        eventBus.deliver(event);

        event = new TestEvent();
        event.message = "bam";

        eventBus.deliver(event);

        Thread.sleep(500);
        

        eventsPromise = monitor.monitorEvents(5, true)
                .collect(Collectors.toList())
                .timeout(2000);

        events = eventsPromise.getValue();
        
        assertEquals(1, events.size(), events.toString());

        assertEquals(TEST_EVENT_TOPIC, events.get(0).topic);

        assertEquals("bam", events.get(0).eventData.get("message"));
    }
    
    @WithConfiguration(pid = "org.apache.aries.typedevent.bus", properties = @Property(key = "event.history.enable.at.start", value = "false", scalar = Scalar.Boolean))
    @Test
    public void testMinimumRetention(@InjectService TypedEventMonitor monitor, 
    		@InjectService TypedEventBus eventBus) throws Exception {
    	
    	monitor.configureHistoryStorage(TEST_EVENT_TOPIC, 3, 5);
    	monitor.configureHistoryStorage("*", 0, Integer.MAX_VALUE);

    	TestEvent event = new TestEvent();
        event.message = "boo";

        eventBus.deliver(event);

        event = new TestEvent();
        event.message = "bam";

        eventBus.deliver(event);
        
        event = new TestEvent();
        event.message = "boom";

        eventBus.deliver(event);

        event = new TestEvent();
        event.message = "foo";
        
        eventBus.deliver(event);

        event = new TestEvent();
        event.message = "bar";
        
        eventBus.deliver(event);

        event = new TestEvent();
        event.message = "foobar";
        
        eventBus.deliver(event);

        Thread.sleep(500);

        Promise<List<MonitorEvent>> eventsPromise = monitor.monitorEvents(6, true)
                .collect(Collectors.toList())
                .timeout(2000);

        List<MonitorEvent> events = eventsPromise.getValue();
        assertEquals(5, events.size(), events.toString());

        assertEquals(TEST_EVENT_TOPIC, events.get(0).topic);
        assertEquals(TEST_EVENT_TOPIC, events.get(1).topic);
        assertEquals(TEST_EVENT_TOPIC, events.get(2).topic);
        assertEquals(TEST_EVENT_TOPIC, events.get(3).topic);
        assertEquals(TEST_EVENT_TOPIC, events.get(4).topic);

        assertEquals("bam", events.get(0).eventData.get("message"));
        assertEquals("boom", events.get(1).eventData.get("message"));
        assertEquals("foo", events.get(2).eventData.get("message"));
        assertEquals("bar", events.get(3).eventData.get("message"));
        assertEquals("foobar", events.get(4).eventData.get("message"));
        
        long maximumEventStorage = monitor.getMaximumEventStorage();
        for(long i = 0; i < maximumEventStorage; i++) {
        	TestEvent2 event2 = new TestEvent2();
        	event2.eventType = EventType.GREEN;
        	event2.subEvent = new TestEvent();
        	event2.subEvent.message = "Hello " + i;
        	eventBus.deliver(event2);
        }
        
        Thread.sleep(500);
        
        eventsPromise = monitor.monitorEvents((int) (maximumEventStorage + 100), true)
                .collect(Collectors.toList())
                .timeout(2000);
        events = eventsPromise.getValue();
        assertEquals(maximumEventStorage, events.size(), events.toString());
        
        events = events.stream()
        		.filter(me -> me.topic.equals(TEST_EVENT_TOPIC))
        		.collect(toList());
        assertEquals(3, events.size(), events.toString());
        assertEquals("foo", events.get(0).eventData.get("message"));
        assertEquals("bar", events.get(1).eventData.get("message"));
        assertEquals("foobar", events.get(2).eventData.get("message"));
        
        monitor.configureHistoryStorage(TEST_EVENT_TOPIC, 1, 2);
        
        eventsPromise = monitor.monitorEvents((int) maximumEventStorage + 100, true)
                .collect(Collectors.toList())
                .timeout(2000);
        events = eventsPromise.getValue();
        assertEquals(maximumEventStorage - 1, events.size(), events.toString());
        
        events = events.stream()
        		.filter(me -> me.topic.equals(TEST_EVENT_TOPIC))
        		.collect(toList());
        assertEquals(2, events.size(), events.toString());
        assertEquals("bar", events.get(0).eventData.get("message"));
        assertEquals("foobar", events.get(1).eventData.get("message"));
    }
}
