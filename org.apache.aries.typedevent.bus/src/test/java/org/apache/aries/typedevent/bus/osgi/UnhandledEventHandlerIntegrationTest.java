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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_FILTER;

import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.aries.typedevent.bus.common.TestEvent;
import org.apache.aries.typedevent.bus.common.TestEventConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.osgi.framework.BundleContext;
import org.osgi.service.typedevent.TypedEventBus;
import org.osgi.service.typedevent.TypedEventHandler;
import org.osgi.service.typedevent.UnhandledEventHandler;
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
public class UnhandledEventHandlerIntegrationTest extends AbstractIntegrationTest {
    
    @InjectBundleContext
    BundleContext context;
    
    @InjectService
    TypedEventBus eventBus;
    
    @Mock
    TestEventConsumer typedEventHandler;

    @Mock
    UntypedEventHandler untypedEventHandler;

    @Mock
    UnhandledEventHandler unhandledEventHandler;

    /**
     * Tests that the unhandledEventHandler gets called appropriately
     * @throws InterruptedException
     */
    @Test
    public void testUnhandledDueToTopic() throws InterruptedException {
        
        Dictionary<String, Object> props = new Hashtable<>();
        
        regs.add(context.registerService(TypedEventHandler.class, typedEventHandler, props));
        
        props = new Hashtable<>();
        
        regs.add(context.registerService(UnhandledEventHandler.class, unhandledEventHandler, props));
        
        
        TestEvent event = new TestEvent();
        event.message = "foo";
        
        eventBus.deliver(event);
        
        verify(typedEventHandler, timeout(1000)).notify(eq(TEST_EVENT_TOPIC), 
                argThat(isTestEventWithMessage("foo")));
        
        verify(unhandledEventHandler, after(1000).never()).notifyUnhandled(anyString(), anyMap());
        
        
        eventBus.deliver("anotherTopic", event);
        
        verify(typedEventHandler, after(1000).never()).notify(eq("anotherTopic"), any());
        
        verify(unhandledEventHandler, timeout(1000)).notifyUnhandled(eq("anotherTopic"), 
                argThat(isUntypedTestEventWithMessage("foo")));

    }

    /**
     * Tests that the consumer of last resort gets called appropriately
     * @throws InterruptedException
     */
    @Test
    public void testUnhandledDueToFilter() throws InterruptedException {
        
        Dictionary<String, Object> props = new Hashtable<>();
        props.put(TYPED_EVENT_FILTER, "(message=foo)");
        
        regs.add(context.registerService(TypedEventHandler.class, typedEventHandler, props));
        
        props = new Hashtable<>();
        
        regs.add(context.registerService(UnhandledEventHandler.class, unhandledEventHandler, props));
        
        
        TestEvent event = new TestEvent();
        event.message = "foo";
        
        eventBus.deliver(event);
        
        verify(typedEventHandler, timeout(1000)).notify(eq(TEST_EVENT_TOPIC), 
                argThat(isTestEventWithMessage("foo")));
        
        verify(unhandledEventHandler, after(1000).never()).notifyUnhandled(anyString(), anyMap());
        
        
        event = new TestEvent();
        event.message = "bar";
        
        
        eventBus.deliver(event);
        
        verify(typedEventHandler, after(1000).never()).notify(eq(TEST_EVENT_TOPIC), 
                argThat(isTestEventWithMessage("bar")));
        
        verify(unhandledEventHandler, timeout(1000)).notifyUnhandled(eq(TEST_EVENT_TOPIC), 
                argThat(isUntypedTestEventWithMessage("bar")));
        
    }
    
}