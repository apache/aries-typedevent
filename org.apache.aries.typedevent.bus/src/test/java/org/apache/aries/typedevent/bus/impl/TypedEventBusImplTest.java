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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.osgi.framework.Constants.SERVICE_ID;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_FILTER;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_HISTORY;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_TOPICS;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_TYPE;
import static org.osgi.util.converter.Converters.standardConverter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.osgi.framework.Bundle;
import org.osgi.framework.Constants;
import org.osgi.service.typedevent.TypedEventHandler;
import org.osgi.service.typedevent.TypedEventPublisher;
import org.osgi.service.typedevent.UnhandledEventHandler;
import org.osgi.service.typedevent.UntypedEventHandler;

@ExtendWith(MockitoExtension.class)
public class TypedEventBusImplTest {

    private static final String SPECIAL_TEST_EVENT_TOPIC = SpecialTestEvent.class.getName().replace(".", "/");

    private static final String TEST_EVENT_TOPIC = TestEvent.class.getName().replace(".", "/");
    private static final String TEST_EVENT_ML_WILDCARD_TOPIC = "org/apache/aries/typedevent/*";
    private static final String BASE_ML_WILDCARD_TOPIC = "*";
    
    private static final String TEST_EVENT_SL_WILDCARD_TOPIC = "org/apache/+/typedevent/bus/impl/TypedEventBusImplTest$TestEvent";
    private static final String BASE_SL_WILDCARD_TOPIC = "+";

    public static class TestEvent {
        public String message;
    }

    public static class TestEvent2 {
        public int count;
    }
    
    public static class SpecialTestEvent extends TestEvent {
        
    }

    @Mock(strictness = LENIENT)
    Bundle registeringBundle;

    @Mock(strictness = LENIENT)
    TypedEventHandler<Object> handlerA, handlerB;

    @Mock(strictness = LENIENT)
    UntypedEventHandler untypedHandlerA, untypedHandlerB;

    @Mock(strictness = LENIENT)
    UnhandledEventHandler unhandledHandler;

    Semaphore semA = new Semaphore(0), semB = new Semaphore(0), untypedSemA = new Semaphore(0),
            untypedSemB = new Semaphore(0), unhandledSem = new Semaphore(0);

    TypedEventBusImpl impl;
    TypedEventMonitorImpl monitorImpl;

    @BeforeEach
    public void start() throws ClassNotFoundException {

        Mockito.doAnswer(i -> TestEvent.class.getClassLoader().loadClass(i.getArgument(0, String.class)))
        	.when(registeringBundle).loadClass(Mockito.anyString());
        
        Mockito.doAnswer(i -> {
            semA.release();
            return null;
        }).when(handlerA).notify(Mockito.anyString(), Mockito.any());

        Mockito.doAnswer(i -> {
            semB.release();
            return null;
        }).when(handlerB).notify(Mockito.anyString(), Mockito.any());

        Mockito.doAnswer(i -> {
            untypedSemA.release();
            return null;
        }).when(untypedHandlerA).notifyUntyped(Mockito.anyString(), Mockito.any());

        Mockito.doAnswer(i -> {
            untypedSemB.release();
            return null;
        }).when(untypedHandlerB).notifyUntyped(Mockito.anyString(), Mockito.any());

        Mockito.doAnswer(i -> {
            unhandledSem.release();
            return null;
        }).when(unhandledHandler).notifyUnhandled(Mockito.anyString(), Mockito.any());

        Map<String, Object> config = Collections.emptyMap();
        
        monitorImpl = new TypedEventMonitorImpl(config);

        impl = new TypedEventBusImpl(monitorImpl, config);
        impl.start();
    }
    
    @AfterEach
    public void stop() throws Exception {
        impl.stop();
        monitorImpl.destroy();
    }

    /**
     * Tests that events are delivered to Smart Behaviours based on type
     * 
     * @throws InterruptedException
     */
    @Test
    public void testEventSending() throws InterruptedException {

        TestEvent event = new TestEvent();
        event.message = "boo";

        Map<String, Object> serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        serviceProperties.put(TYPED_EVENT_TYPE, TestEvent.class.getName());
        serviceProperties.put(SERVICE_ID, 42L);

        impl.addTypedEventHandler(registeringBundle, handlerA, serviceProperties);

        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TestEvent2.class.getName().replace(".", "/"));
        serviceProperties.put(TYPED_EVENT_TYPE, TestEvent2.class.getName());
        serviceProperties.put(SERVICE_ID, 43L);

        impl.addTypedEventHandler(registeringBundle, handlerB, serviceProperties);

        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        serviceProperties.put(SERVICE_ID, 44L);

        impl.addUntypedEventHandler(untypedHandlerA, serviceProperties);

        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TestEvent2.class.getName().replace(".", "/"));
        serviceProperties.put(SERVICE_ID, 45L);

        impl.addUntypedEventHandler(untypedHandlerB, serviceProperties);

        impl.deliver(event);

        assertTrue(semA.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(handlerA).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isTestEventWithMessage("boo")));

        assertFalse(semB.tryAcquire(1, TimeUnit.SECONDS));

        assertTrue(untypedSemA.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(untypedHandlerA).notifyUntyped(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isUntypedTestEventWithMessage("boo")));

        assertFalse(untypedSemB.tryAcquire(1, TimeUnit.SECONDS));

    }

    /**
     * Tests that events are delivered with Wildcarding
     * 
     * @throws InterruptedException
     */
    @Test
    public void testMultiLevelWildcardEventReceiving() throws InterruptedException {
    	
    	TestEvent event = new TestEvent();
    	event.message = "boo";
    	
    	Map<String, Object> serviceProperties = new HashMap<>();
    	
    	serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_ML_WILDCARD_TOPIC);
    	serviceProperties.put(TYPED_EVENT_TYPE, TestEvent.class.getName());
    	serviceProperties.put(SERVICE_ID, 42L);
    	
    	impl.addTypedEventHandler(registeringBundle, handlerA, serviceProperties);
    	
    	serviceProperties = new HashMap<>();
    	
    	serviceProperties.put(TYPED_EVENT_TOPICS, BASE_ML_WILDCARD_TOPIC);
    	serviceProperties.put(TYPED_EVENT_TYPE, TestEvent.class.getName());
    	serviceProperties.put(SERVICE_ID, 43L);
    	
    	impl.addTypedEventHandler(registeringBundle, handlerB, serviceProperties);
    	
    	serviceProperties = new HashMap<>();
    	
    	serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_ML_WILDCARD_TOPIC);
    	serviceProperties.put(SERVICE_ID, 44L);
    	
    	impl.addUntypedEventHandler(untypedHandlerA, serviceProperties);
    	
    	serviceProperties = new HashMap<>();
    	
    	serviceProperties.put(TYPED_EVENT_TOPICS, BASE_ML_WILDCARD_TOPIC);
    	serviceProperties.put(SERVICE_ID, 45L);
    	
    	impl.addUntypedEventHandler(untypedHandlerB, serviceProperties);
    	
    	impl.deliver(event);
    	
    	assertTrue(semA.tryAcquire(1, TimeUnit.SECONDS));
    	
    	Mockito.verify(handlerA).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
    			Mockito.argThat(isTestEventWithMessage("boo")));
    	
    	assertTrue(semB.tryAcquire(1, TimeUnit.SECONDS));
    	
    	Mockito.verify(handlerB).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
    			Mockito.argThat(isTestEventWithMessage("boo")));
    	
    	assertTrue(untypedSemA.tryAcquire(1, TimeUnit.SECONDS));
    	
    	Mockito.verify(untypedHandlerA).notifyUntyped(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
    			Mockito.argThat(isUntypedTestEventWithMessage("boo")));
    	
    	assertTrue(untypedSemB.tryAcquire(1, TimeUnit.SECONDS));
    	
    	Mockito.verify(untypedHandlerB).notifyUntyped(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
    			Mockito.argThat(isUntypedTestEventWithMessage("boo")));

    	impl.deliver("some/other/topic", event);
    	
    	assertFalse(semA.tryAcquire(1, TimeUnit.SECONDS));
    	
    	assertTrue(semB.tryAcquire(1, TimeUnit.SECONDS));
    	
    	Mockito.verify(handlerB).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
    			Mockito.argThat(isTestEventWithMessage("boo")));
    	
    	assertFalse(untypedSemA.tryAcquire(1, TimeUnit.SECONDS));
    	
    	assertTrue(untypedSemB.tryAcquire(1, TimeUnit.SECONDS));
    	
    	Mockito.verify(untypedHandlerB).notifyUntyped(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
    			Mockito.argThat(isUntypedTestEventWithMessage("boo")));
    }

    /**
     * Tests that events are delivered with Single Level wildcards
     * 
     * @throws InterruptedException
     */
    @Test
    public void testSingleLevelWildcardEventReceiving() throws InterruptedException {
    	
    	TestEvent event = new TestEvent();
    	event.message = "boo";
    	
    	Map<String, Object> serviceProperties = new HashMap<>();
    	
    	serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_SL_WILDCARD_TOPIC);
    	serviceProperties.put(TYPED_EVENT_TYPE, TestEvent.class.getName());
    	serviceProperties.put(SERVICE_ID, 42L);
    	
    	impl.addTypedEventHandler(registeringBundle, handlerA, serviceProperties);
    	
    	serviceProperties = new HashMap<>();
    	
    	serviceProperties.put(TYPED_EVENT_TOPICS, BASE_SL_WILDCARD_TOPIC);
    	serviceProperties.put(TYPED_EVENT_TYPE, TestEvent.class.getName());
    	serviceProperties.put(SERVICE_ID, 43L);
    	
    	impl.addTypedEventHandler(registeringBundle, handlerB, serviceProperties);
    	
    	serviceProperties = new HashMap<>();
    	
    	serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_SL_WILDCARD_TOPIC);
    	serviceProperties.put(SERVICE_ID, 44L);
    	
    	impl.addUntypedEventHandler(untypedHandlerA, serviceProperties);
    	
    	serviceProperties = new HashMap<>();
    	
    	serviceProperties.put(TYPED_EVENT_TOPICS, BASE_SL_WILDCARD_TOPIC);
    	serviceProperties.put(SERVICE_ID, 45L);
    	
    	impl.addUntypedEventHandler(untypedHandlerB, serviceProperties);
    	
    	impl.deliver(event);
    	
    	assertTrue(semA.tryAcquire(1, TimeUnit.SECONDS));
    	
    	Mockito.verify(handlerA).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
    			Mockito.argThat(isTestEventWithMessage("boo")));
    	
    	assertFalse(semB.tryAcquire(1, TimeUnit.SECONDS));
    	
    	assertTrue(untypedSemA.tryAcquire(1, TimeUnit.SECONDS));
    	
    	Mockito.verify(untypedHandlerA).notifyUntyped(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
    			Mockito.argThat(isUntypedTestEventWithMessage("boo")));
    	
    	assertFalse(untypedSemB.tryAcquire(1, TimeUnit.SECONDS));
    	
    	String topic = "org/apache/taurus/typedevent/bus/impl/TypedEventBusImplTest$TestEvent";
		impl.deliver(topic, event);
    	
    	assertTrue(semA.tryAcquire(1, TimeUnit.SECONDS));
    	
    	Mockito.verify(handlerA).notify(Mockito.eq(topic),
    			Mockito.argThat(isTestEventWithMessage("boo")));

    	assertFalse(semB.tryAcquire(1, TimeUnit.SECONDS));
    	
    	assertTrue(untypedSemA.tryAcquire(1, TimeUnit.SECONDS));

    	Mockito.verify(untypedHandlerA).notifyUntyped(Mockito.eq(topic),
    			Mockito.argThat(isUntypedTestEventWithMessage("boo")));
    	
    	assertFalse(untypedSemB.tryAcquire(1, TimeUnit.SECONDS));

    	topic = "org";
    	impl.deliver(topic, event);
    	
    	assertFalse(semA.tryAcquire(1, TimeUnit.SECONDS));

    	assertTrue(semB.tryAcquire(1, TimeUnit.SECONDS));
    	
    	Mockito.verify(handlerB).notify(Mockito.eq(topic),
    			Mockito.argThat(isTestEventWithMessage("boo")));
    	
    	assertFalse(untypedSemA.tryAcquire(1, TimeUnit.SECONDS));

    	assertTrue(untypedSemB.tryAcquire(1, TimeUnit.SECONDS));
    	
    	Mockito.verify(untypedHandlerB).notifyUntyped(Mockito.eq(topic),
    			Mockito.argThat(isUntypedTestEventWithMessage("boo")));
    	
    }
    
    public static class TestEventHandler implements TypedEventHandler<TestEvent> {

        @Override
        public void notify(String topic, TestEvent event) {
            // No op
        }
    }

    public static interface TestEventHandlerIface extends TypedEventHandler<TestEvent> {
        
    }
    
    /**
     * Tests that reified typedEventHandlers are properly processed
     * 
     * @throws InterruptedException
     */
    @Test
    public void testGenericTypeInference() throws InterruptedException {
        
        TypedEventHandler<TestEvent> handler = Mockito.spy(TestEventHandler.class);
        TypedEventHandler<TestEvent> handler2 = Mockito.spy(TestEventHandler.class);
        TypedEventHandler<TestEvent> handler3 = Mockito.mock(TestEventHandlerIface.class);
        
        TestEvent event = new TestEvent();
        event.message = "boo";
        
        Map<String, Object> serviceProperties = new HashMap<>();
        serviceProperties.put(SERVICE_ID, 42L);
        
        impl.addTypedEventHandler(registeringBundle, handler, serviceProperties);
        
        serviceProperties = new HashMap<>();
        
        serviceProperties.put(TYPED_EVENT_TYPE, SpecialTestEvent.class.getName());
        serviceProperties.put(SERVICE_ID, 43L);
        
        impl.addTypedEventHandler(registeringBundle, handler2, serviceProperties);

        serviceProperties = new HashMap<>();
        
        serviceProperties.put(SERVICE_ID, 44L);
        
        impl.addTypedEventHandler(registeringBundle, handler3, serviceProperties);
        
        impl.deliver(event);
        
        Mockito.verify(handler, Mockito.timeout(1000)).notify(eq(TEST_EVENT_TOPIC), argThat(isTestEventWithMessage("boo")));
        Mockito.verify(handler3, Mockito.timeout(1000)).notify(eq(TEST_EVENT_TOPIC), argThat(isTestEventWithMessage("boo")));
        
        Mockito.verify(handler2, Mockito.after(1000).never()).notify(Mockito.anyString(), Mockito.any());

        
        event = new SpecialTestEvent();
        event.message = "far";
        impl.deliver(event);
        
        Mockito.verify(handler, Mockito.after(1000).never()).notify(eq(SPECIAL_TEST_EVENT_TOPIC), Mockito.any());
        Mockito.verify(handler3, Mockito.after(1000).never()).notify(eq(SPECIAL_TEST_EVENT_TOPIC), Mockito.any());
        
        Mockito.verify(handler2, Mockito.timeout(1000)).notify(eq(SPECIAL_TEST_EVENT_TOPIC), 
                argThat(isSpecialTestEventWithMessage("far")));
        
        
        
    }

    /**
     * Tests that events are delivered to Event Handlers based on type
     * 
     * @throws InterruptedException
     */
    @Test
    public void testUntypedEventSending() throws InterruptedException {

        TestEvent event = new TestEvent();
        event.message = "boo";

        Map<String, Object> serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TestEvent.class.getName().replace('.', '/'));
        serviceProperties.put(TYPED_EVENT_TYPE, TestEvent.class.getName());
        serviceProperties.put(SERVICE_ID, 42L);

        impl.addTypedEventHandler(registeringBundle, handlerA, serviceProperties);

        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TestEvent2.class.getName().replace('.', '/'));
        serviceProperties.put(TYPED_EVENT_TYPE, TestEvent2.class.getName());
        serviceProperties.put(SERVICE_ID, 43L);

        impl.addTypedEventHandler(registeringBundle, handlerB, serviceProperties);

        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TestEvent.class.getName().replace('.', '/'));
        serviceProperties.put(SERVICE_ID, 44L);

        impl.addUntypedEventHandler(untypedHandlerA, serviceProperties);

        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TestEvent2.class.getName().replace('.', '/'));
        serviceProperties.put(SERVICE_ID, 45L);

        impl.addUntypedEventHandler(untypedHandlerB, serviceProperties);

        impl.deliver(event.getClass().getName().replace('.', '/'), standardConverter().convert(event).to(Map.class));

        assertTrue(semA.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(handlerA).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isTestEventWithMessage("boo")));

        assertFalse(semB.tryAcquire(1, TimeUnit.SECONDS));

        assertTrue(untypedSemA.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(untypedHandlerA).notifyUntyped(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isUntypedTestEventWithMessage("boo")));

        assertFalse(untypedSemB.tryAcquire(1, TimeUnit.SECONDS));

    }

    /**
     * Tests that filtering is applied to message sending/receiving
     * 
     * @throws InterruptedException
     */
    @Test
    public void testEventFiltering() throws InterruptedException {

        Map<String, Object> serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        serviceProperties.put(TYPED_EVENT_TYPE, TestEvent.class.getName());
        serviceProperties.put(TYPED_EVENT_FILTER, "(message=foo)");
        serviceProperties.put(SERVICE_ID, 42L);

        impl.addTypedEventHandler(registeringBundle, handlerA, serviceProperties);

        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        serviceProperties.put(TYPED_EVENT_TYPE, TestEvent.class.getName());
        serviceProperties.put(TYPED_EVENT_FILTER, "(message=bar)");
        serviceProperties.put(SERVICE_ID, 43L);

        impl.addTypedEventHandler(registeringBundle, handlerB, serviceProperties);

        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        serviceProperties.put(TYPED_EVENT_FILTER, "(message=foo)");
        serviceProperties.put(SERVICE_ID, 44L);

        impl.addUntypedEventHandler(untypedHandlerA, serviceProperties);

        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        serviceProperties.put(TYPED_EVENT_FILTER, "(message=bar)");
        serviceProperties.put(SERVICE_ID, 45L);

        impl.addUntypedEventHandler(untypedHandlerB, serviceProperties);

        TestEvent event = new TestEvent();
        event.message = "foo";

        impl.deliver(event);

        assertTrue(semA.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(handlerA).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isTestEventWithMessage("foo")));

        assertFalse(semB.tryAcquire(1, TimeUnit.SECONDS));

        assertTrue(untypedSemA.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(untypedHandlerA).notifyUntyped(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isUntypedTestEventWithMessage("foo")));

        assertFalse(untypedSemB.tryAcquire(1, TimeUnit.SECONDS));

        event = new TestEvent();
        event.message = "bar";

        impl.deliver(event);

        assertTrue(semB.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(handlerB).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isTestEventWithMessage("bar")));

        assertFalse(semA.tryAcquire(1, TimeUnit.SECONDS));

        assertTrue(untypedSemB.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(untypedHandlerB).notifyUntyped(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isUntypedTestEventWithMessage("bar")));

        assertFalse(untypedSemA.tryAcquire(1, TimeUnit.SECONDS));
    }

    /**
     * Tests that filtering is applied to message sending/receiving
     * 
     * @throws InterruptedException
     */
    @Test
    public void testEventFilteringWithEmptyStringFilter() throws InterruptedException {

        Map<String, Object> serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        serviceProperties.put(TYPED_EVENT_TYPE, TestEvent.class.getName());
        serviceProperties.put(TYPED_EVENT_FILTER, "");
        serviceProperties.put(SERVICE_ID, 42L);

        impl.addTypedEventHandler(registeringBundle, handlerA, serviceProperties);

        TestEvent event = new TestEvent();
        event.message = "foo";

        impl.deliver(event);

        assertTrue(semA.tryAcquire(1, TimeUnit.SECONDS));
    }

    /**
     * Tests that the consumer of last resort gets called appropriately
     * 
     * @throws InterruptedException
     */
    @Test
    public void testUnhandledEventHandlers() throws InterruptedException {

        Map<String, Object> serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        serviceProperties.put(TYPED_EVENT_TYPE, TestEvent.class.getName());
        serviceProperties.put(TYPED_EVENT_FILTER, "(message=foo)");
        serviceProperties.put(SERVICE_ID, 42L);

        impl.addTypedEventHandler(registeringBundle, handlerA, serviceProperties);

        serviceProperties = new HashMap<>();

        serviceProperties.put(Constants.SERVICE_ID, 45L);

        impl.addUnhandledEventHandler(unhandledHandler, serviceProperties);

        TestEvent event = new TestEvent();
        event.message = "foo";

        impl.deliver(event);

        assertTrue(semA.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(handlerA).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isTestEventWithMessage("foo")));

        assertFalse(unhandledSem.tryAcquire(1, TimeUnit.SECONDS));

        event = new TestEvent();
        event.message = "bar";

        impl.deliver(event);

        assertTrue(unhandledSem.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(unhandledHandler).notifyUnhandled(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isUntypedTestEventWithMessage("bar")));

        assertFalse(semA.tryAcquire(1, TimeUnit.SECONDS));

    }
    
    /**
     * Tests that events are delivered correctly using a publisher
     * 
     * @throws InterruptedException
     */
    @Test
    public void testEventPublisher() throws InterruptedException {

        TestEvent event = new TestEvent();
        event.message = "boo";

        Map<String, Object> serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        serviceProperties.put(TYPED_EVENT_TYPE, TestEvent.class.getName());
        serviceProperties.put(SERVICE_ID, 42L);

        impl.addTypedEventHandler(registeringBundle, handlerA, serviceProperties);

        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TestEvent2.class.getName().replace(".", "/"));
        serviceProperties.put(TYPED_EVENT_TYPE, TestEvent2.class.getName());
        serviceProperties.put(SERVICE_ID, 43L);

        impl.addTypedEventHandler(registeringBundle, handlerB, serviceProperties);

        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        serviceProperties.put(SERVICE_ID, 44L);

        impl.addUntypedEventHandler(untypedHandlerA, serviceProperties);

        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TestEvent2.class.getName().replace(".", "/"));
        serviceProperties.put(SERVICE_ID, 45L);

        impl.addUntypedEventHandler(untypedHandlerB, serviceProperties);

        TypedEventPublisher<TestEvent> publisher = impl.createPublisher(TestEvent.class);
        
        publisher.deliver(event);

        assertTrue(semA.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(handlerA).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isTestEventWithMessage("boo")));

        assertFalse(semB.tryAcquire(1, TimeUnit.SECONDS));

        assertTrue(untypedSemA.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(untypedHandlerA).notifyUntyped(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isUntypedTestEventWithMessage("boo")));

        assertFalse(untypedSemB.tryAcquire(1, TimeUnit.SECONDS));
        
        
        Map<String, Object> eventMap = Map.of("message", "far");
        publisher.deliverUntyped(eventMap);

        assertTrue(semA.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(handlerA).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isTestEventWithMessage("far")));

        assertFalse(semB.tryAcquire(1, TimeUnit.SECONDS));

        assertTrue(untypedSemA.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(untypedHandlerA).notifyUntyped(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isUntypedTestEventWithMessage("far")));

        assertFalse(untypedSemB.tryAcquire(1, TimeUnit.SECONDS));        
        
        assertTrue(publisher.isOpen());
        publisher.close();
        assertFalse(publisher.isOpen());
        
        assertThrows(IllegalStateException.class, () -> publisher.deliver(event));
        assertThrows(IllegalStateException.class, () -> publisher.deliverUntyped(eventMap));
    }

    /**
     * Tests that filtering is applied when delivering historical events
     * 
     * @throws InterruptedException
     */
    @Test
    public void testEventHistoryFiltering() throws InterruptedException {

    	TestEvent event = new TestEvent();
    	event.message = "foo";
    	
    	impl.deliver(event);
    	
    	event = new TestEvent();
        event.message = "bar";

        impl.deliver(event);
        
        event = new TestEvent();
        event.message = "foobar";

        impl.deliver(event);

        event = new TestEvent();
        event.message = "barfoo";
        
        impl.deliver(event);
    	
    	Map<String, Object> serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        serviceProperties.put(TYPED_EVENT_TYPE, TestEvent.class.getName());
        serviceProperties.put(TYPED_EVENT_FILTER, "(message=foo*)");
        serviceProperties.put(TYPED_EVENT_HISTORY, 4);
        serviceProperties.put(SERVICE_ID, 42L);

        impl.addTypedEventHandler(registeringBundle, handlerA, serviceProperties);
        
        assertTrue(semA.tryAcquire(2, 1, TimeUnit.SECONDS));
        assertFalse(semA.tryAcquire(1, TimeUnit.SECONDS));
        
        InOrder order = Mockito.inOrder(handlerA);
        order.verify(handlerA).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isTestEventWithMessage("foo")));
        order.verify(handlerA).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
        		Mockito.argThat(isTestEventWithMessage("foobar")));

        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        serviceProperties.put(TYPED_EVENT_TYPE, TestEvent.class.getName());
        serviceProperties.put(TYPED_EVENT_FILTER, "(|(message=bar*)(message=foo*))");
        serviceProperties.put(TYPED_EVENT_HISTORY, 3);
        serviceProperties.put(SERVICE_ID, 43L);

        impl.addTypedEventHandler(registeringBundle, handlerB, serviceProperties);

        assertTrue(semB.tryAcquire(3, 1, TimeUnit.SECONDS));
        assertFalse(semB.tryAcquire(1, TimeUnit.SECONDS));
        
        order = Mockito.inOrder(handlerB);
        order.verify(handlerB).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
        		Mockito.argThat(isTestEventWithMessage("bar")));
        order.verify(handlerB).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
        		Mockito.argThat(isTestEventWithMessage("foobar")));
        order.verify(handlerB).notify(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
        		Mockito.argThat(isTestEventWithMessage("barfoo")));
        
        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        serviceProperties.put(TYPED_EVENT_FILTER, "(|(message=foo)(message=bar))");
        serviceProperties.put(TYPED_EVENT_HISTORY, 4);
        serviceProperties.put(SERVICE_ID, 44L);

        impl.addUntypedEventHandler(untypedHandlerA, serviceProperties);

        assertTrue(untypedSemA.tryAcquire(2, 1, TimeUnit.SECONDS));
        assertFalse(untypedSemA.tryAcquire(1, TimeUnit.SECONDS));
        
        Mockito.verify(untypedHandlerA).notifyUntyped(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
        		Mockito.argThat(isUntypedTestEventWithMessage("foo")));
        Mockito.verify(untypedHandlerA).notifyUntyped(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
        		Mockito.argThat(isUntypedTestEventWithMessage("bar")));

        serviceProperties = new HashMap<>();

        serviceProperties.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        serviceProperties.put(TYPED_EVENT_FILTER, "(message=*)");
        serviceProperties.put(TYPED_EVENT_HISTORY, 1);
        serviceProperties.put(SERVICE_ID, 45L);

        impl.addUntypedEventHandler(untypedHandlerB, serviceProperties);

        assertTrue(untypedSemB.tryAcquire(1, TimeUnit.SECONDS));
        assertFalse(untypedSemB.tryAcquire(1, TimeUnit.SECONDS));

        Mockito.verify(untypedHandlerB).notifyUntyped(Mockito.eq(TestEvent.class.getName().replace('.', '/')),
                Mockito.argThat(isUntypedTestEventWithMessage("barfoo")));
    }

    ArgumentMatcher<TestEvent> isTestEventWithMessage(String message) {
        return new ArgumentMatcher<TestEvent>() {

            @Override
            public boolean matches(TestEvent argument) {
                return argument instanceof TestEvent && message.equals(((TestEvent) argument).message);
            }
        };
    }
    
    ArgumentMatcher<SpecialTestEvent> isSpecialTestEventWithMessage(String message) {
        return new ArgumentMatcher<SpecialTestEvent>() {
            
            @Override
            public boolean matches(SpecialTestEvent argument) {
                return argument instanceof SpecialTestEvent && message.equals(((SpecialTestEvent) argument).message);
            }
        };
    }

    ArgumentMatcher<Map<String, Object>> isUntypedTestEventWithMessage(String message) {
        return new ArgumentMatcher<Map<String, Object>>() {

            @Override
            public boolean matches(Map<String, Object> argument) {
                return argument != null && message.equals(argument.get("message"));
            }
        };
    }

}
