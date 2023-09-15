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
package org.apache.aries.typedevent.bus.osgi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_TOPICS;

import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.aries.typedevent.bus.common.TestEvent;
import org.apache.aries.typedevent.bus.common.TestEvent2;
import org.apache.aries.typedevent.bus.common.TestEvent2.EventType;
import org.apache.aries.typedevent.bus.common.TestEvent2Consumer;
import org.apache.aries.typedevent.bus.common.TestEventConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.osgi.framework.BundleContext;
import org.osgi.service.typedevent.TypedEventBus;
import org.osgi.service.typedevent.TypedEventConstants;
import org.osgi.service.typedevent.TypedEventHandler;
import org.osgi.service.typedevent.UntypedEventHandler;
import org.osgi.test.common.annotation.InjectBundleContext;
import org.osgi.test.common.annotation.InjectService;
import org.osgi.test.junit5.context.BundleContextExtension;
import org.osgi.test.junit5.service.ServiceExtension;

/**
 * This is a JUnit test that will be run inside an OSGi framework.
 * 
 * It can interact with the framework by starting or stopping bundles,
 * getting or registering services, or in other ways, and then observing
 * the result on the bundle(s) being tested.
 */
@ExtendWith(BundleContextExtension.class)
@ExtendWith(ServiceExtension.class)
@ExtendWith(MockitoExtension.class)
public class EventDeliveryIntegrationTest extends AbstractIntegrationTest {
    
    @InjectBundleContext
    BundleContext context;
    
    @InjectService
    TypedEventBus eventBus;
    
    @Mock
    TestEventConsumer typedEventHandler;

    @Mock
    TestEvent2Consumer typedEventHandler2;

    @Mock
    UntypedEventHandler untypedEventHandler, untypedEventHandler2;
    
    /**
     * Tests that events are delivered to untyped Event Handlers
     * based on topic
     * 
     * @throws InterruptedException
     */
    @Test
    public void testEventReceiving() throws InterruptedException {
        
        TestEvent event = new TestEvent();
        event.message = "boo";
        
        Dictionary<String, Object> props = new Hashtable<>();
        
        regs.add(context.registerService(TypedEventHandler.class, typedEventHandler, props));

        regs.add(context.registerService(TypedEventHandler.class, typedEventHandler2, props));
        
        eventBus.deliver(event);
        
        Mockito.verify(typedEventHandler, Mockito.timeout(1000)).notify(
                Mockito.eq(TEST_EVENT_TOPIC), Mockito.argThat(isTestEventWithMessage("boo")));

        Mockito.verify(typedEventHandler2, Mockito.after(1000).never()).notify(
                Mockito.eq(TEST_EVENT_TOPIC), Mockito.any());
    }

    /**
     * Tests that events are delivered to untyped Event Handlers
     * based on topic
     * 
     * @throws InterruptedException
     */
    @Test
    public void testEventReceivingUntyped() throws InterruptedException {
        
        TestEvent event = new TestEvent();
        event.message = "boo";
        
        Dictionary<String, Object> props = new Hashtable<>();
        props.put(TypedEventConstants.TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        
        regs.add(context.registerService(UntypedEventHandler.class, untypedEventHandler, props));
        
        props = new Hashtable<>();
        
        props.put(TypedEventConstants.TYPED_EVENT_TOPICS, TEST_EVENT_2_TOPIC);
        
        regs.add(context.registerService(UntypedEventHandler.class, untypedEventHandler2, props));
        
        
        eventBus.deliver(event);
        
        Mockito.verify(untypedEventHandler, Mockito.timeout(1000)).notifyUntyped(
                Mockito.eq(TEST_EVENT_TOPIC), Mockito.argThat(isUntypedTestEventWithMessage("boo")));

        Mockito.verify(untypedEventHandler2, Mockito.after(1000).never()).notifyUntyped(
                Mockito.eq(TEST_EVENT_TOPIC), Mockito.argThat(isUntypedTestEventWithMessage("boo")));
        
    }
    
    @Test
    public void testSendComplexEvent() throws Exception {
        Dictionary<String, Object> props = new Hashtable<>();
        
        regs.add(context.registerService(TypedEventHandler.class, typedEventHandler2, props));
        
        TestEvent event = new TestEvent();
        event.message = "foo";
        
        TestEvent2 event2 = TestEvent2.create(event);
        
        eventBus.deliver(event2);
        
        
        Mockito.verify(typedEventHandler2, Mockito.timeout(1000))
            .notify(Mockito.eq(TEST_EVENT_2_TOPIC), Mockito.argThat(isTestEvent2WithMessage("foo")));
    }

    @Test
    public void testSendComplexEventToUntypedReceiver() throws Exception {
        Dictionary<String, Object> props = new Hashtable<>();
        props.put(TypedEventConstants.TYPED_EVENT_TOPICS, TEST_EVENT_2_TOPIC);
        
        regs.add(context.registerService(UntypedEventHandler.class, 
                untypedEventHandler, props));
        
        TestEvent event = new TestEvent();
        event.message = "foo";
        
        TestEvent2 event2 = TestEvent2.create(event);
        
        eventBus.deliver(event2);
        
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        
        Mockito.verify(untypedEventHandler, Mockito.timeout(1000))
            .notifyUntyped(eq(TEST_EVENT_2_TOPIC), captor.capture());
        
        Map<String, Object> map = captor.getValue();
        
        // Should be a String not an enum as we can't see the types
        assertEquals("RED", map.get("eventType"));
        @SuppressWarnings("unchecked")
        Map<String, Object> subMap = (Map<String, Object>) map.get("subEvent");
        
        assertEquals("foo", subMap.get("message"));
    }
    
    @Test
    public void testSendComplexUntypedEventToTypedReceiver() throws Exception {
        Dictionary<String, Object> props = new Hashtable<>();
        
        regs.add(context.registerService(TypedEventHandler.class, 
                typedEventHandler2, props));
        
        Map<String, Object> event = new HashMap<>();
        event.put("message", "foo");
        
        Map<String, Object> event2 = new HashMap<>();
        event2.put("subEvent", event);
        event2.put("eventType", "BLUE");
        
        eventBus.deliver(TEST_EVENT_2_TOPIC, event2);
        
        ArgumentCaptor<TestEvent2> captor = ArgumentCaptor.forClass(TestEvent2.class);
        
        Mockito.verify(typedEventHandler2, Mockito.timeout(1000))
            .notify(eq(TEST_EVENT_2_TOPIC), captor.capture());
        
        TestEvent2 received = captor.getValue();
        
        // Should be a String not an enum as we can't see the types
        assertEquals(EventType.BLUE, received.eventType);
        
        assertEquals("foo", received.subEvent.message);
    }
    
    /**
     * Tests that events are delivered to untyped Event Handlers
     * based on topic
     * 
     * @throws InterruptedException
     */
    @Test
    public void testEventReceivingUpdateTopic() throws InterruptedException {
        
        TestEvent event = new TestEvent();
        event.message = "boo";
        
        Dictionary<String, Object> props = new Hashtable<>();
        props.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        
        regs.add(context.registerService(TypedEventHandler.class, typedEventHandler, props));

        regs.add(context.registerService(UntypedEventHandler.class, untypedEventHandler, props));
        
        eventBus.deliver(event);
        
        Mockito.verify(typedEventHandler, Mockito.timeout(1000)).notify(
                Mockito.eq(TEST_EVENT_TOPIC), Mockito.argThat(isTestEventWithMessage("boo")));

        Mockito.verify(untypedEventHandler, Mockito.timeout(1000)).notifyUntyped(
                Mockito.eq(TEST_EVENT_TOPIC), Mockito.argThat(isUntypedTestEventWithMessage("boo")));
        
        Mockito.clearInvocations(typedEventHandler, untypedEventHandler);
        
        props.put(TYPED_EVENT_TOPICS, TEST_EVENT_2_TOPIC);
        
        regs.forEach(s -> s.setProperties(props));
        
        eventBus.deliver(event);
        
        Mockito.verify(typedEventHandler, Mockito.after(1000).never()).notify(
                Mockito.eq(TEST_EVENT_TOPIC), Mockito.any());
        Mockito.verify(untypedEventHandler, Mockito.after(1000).never()).notifyUntyped(
        		Mockito.eq(TEST_EVENT_TOPIC), Mockito.any());
        
        eventBus.deliver(TEST_EVENT_2_TOPIC, event);
        
        Mockito.verify(typedEventHandler, Mockito.timeout(1000)).notify(
                Mockito.eq(TEST_EVENT_2_TOPIC), Mockito.argThat(isTestEventWithMessage("boo")));

        Mockito.verify(untypedEventHandler, Mockito.timeout(1000)).notifyUntyped(
                Mockito.eq(TEST_EVENT_2_TOPIC), Mockito.argThat(isUntypedTestEventWithMessage("boo")));
        
    }

    /**
     * Tests that events are delivered to untyped Event Handlers
     * based on topic
     * 
     * @throws InterruptedException
     */
    @Test
    public void testEventReceivingUpdateWildcardTopic() throws InterruptedException {
    	
    	TestEvent event = new TestEvent();
    	event.message = "boo";
    	
    	Dictionary<String, Object> props = new Hashtable<>();
    	props.put(TYPED_EVENT_TOPICS, "foo/+/foobar");
    	
    	regs.add(context.registerService(TypedEventHandler.class, typedEventHandler, props));
    	
    	regs.add(context.registerService(UntypedEventHandler.class, untypedEventHandler, props));
    	
    	eventBus.deliver("foo/bar/foobar", event);
    	
    	Mockito.verify(typedEventHandler, Mockito.timeout(1000)).notify(
    			Mockito.eq("foo/bar/foobar"), Mockito.argThat(isTestEventWithMessage("boo")));
    	
    	Mockito.verify(untypedEventHandler, Mockito.timeout(1000)).notifyUntyped(
    			Mockito.eq("foo/bar/foobar"), Mockito.argThat(isUntypedTestEventWithMessage("boo")));
    	
    	Mockito.clearInvocations(typedEventHandler, untypedEventHandler);
    	
    	props.put(TYPED_EVENT_TOPICS, "foo/bar/foobar/+");
    	
    	regs.forEach(s -> s.setProperties(props));
    	
    	eventBus.deliver("foo/bar/foobar", event);
    	
    	Mockito.verify(typedEventHandler, Mockito.after(1000).never()).notify(
    			Mockito.eq("foo/bar/foobar"), Mockito.any());
    	Mockito.verify(untypedEventHandler, Mockito.after(1000).never()).notifyUntyped(
    			Mockito.eq("foo/bar/foobar"), Mockito.any());
    	
    	eventBus.deliver("foo/bar/foobar/fizzbuzz", event);
    	
    	Mockito.verify(typedEventHandler, Mockito.timeout(1000)).notify(
    			Mockito.eq("foo/bar/foobar/fizzbuzz"), Mockito.argThat(isTestEventWithMessage("boo")));
    	
    	Mockito.verify(untypedEventHandler, Mockito.timeout(1000)).notifyUntyped(
    			Mockito.eq("foo/bar/foobar/fizzbuzz"), Mockito.argThat(isUntypedTestEventWithMessage("boo")));
    	
    }
    
}