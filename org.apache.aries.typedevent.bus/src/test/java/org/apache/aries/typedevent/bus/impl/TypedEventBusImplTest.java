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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.osgi.framework.Constants.SERVICE_ID;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_FILTER;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_TOPICS;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_TYPE;
import static org.osgi.util.converter.Converters.standardConverter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.osgi.framework.Bundle;
import org.osgi.framework.Constants;
import org.osgi.service.typedevent.TypedEventHandler;
import org.osgi.service.typedevent.UnhandledEventHandler;
import org.osgi.service.typedevent.UntypedEventHandler;

public class TypedEventBusImplTest {

    private static final String SPECIAL_TEST_EVENT_TOPIC = SpecialTestEvent.class.getName().replace(".", "/");

    private static final String TEST_EVENT_TOPIC = TestEvent.class.getName().replace(".", "/");

    public static class TestEvent {
        public String message;
    }

    public static class TestEvent2 {
        public int count;
    }
    
    public static class SpecialTestEvent extends TestEvent {
        
    }

    @Mock(lenient = true)
    Bundle registeringBundle;

    @Mock(lenient = true)
    TypedEventHandler<Object> handlerA, handlerB;

    @Mock(lenient = true)
    UntypedEventHandler untypedHandlerA, untypedHandlerB;

    @Mock(lenient = true)
    UnhandledEventHandler unhandledHandler;

    Semaphore semA = new Semaphore(0), semB = new Semaphore(0), untypedSemA = new Semaphore(0),
            untypedSemB = new Semaphore(0), unhandledSem = new Semaphore(0);

    TypedEventBusImpl impl;
    TypedEventMonitorImpl monitorImpl;

    private AutoCloseable mocks;

    @BeforeEach
    public void start() throws ClassNotFoundException {

        mocks = MockitoAnnotations.openMocks(this);
        
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

        monitorImpl = new TypedEventMonitorImpl(new HashMap<String, Object>());

        impl = new TypedEventBusImpl(monitorImpl, new HashMap<String, Object>());
        impl.start();
    }

    @AfterEach
    public void stop() throws Exception {
        impl.stop();
        monitorImpl.destroy();
        mocks.close();
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
