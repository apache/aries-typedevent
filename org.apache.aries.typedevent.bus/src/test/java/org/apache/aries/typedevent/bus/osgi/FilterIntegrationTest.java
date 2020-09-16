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

import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.aries.typedevent.bus.common.TestEvent;
import org.apache.aries.typedevent.bus.common.TestEventConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.osgi.framework.BundleContext;
import org.osgi.service.typedevent.TypedEventBus;
import org.osgi.service.typedevent.TypedEventHandler;
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
public class FilterIntegrationTest extends AbstractIntegrationTest {
    
    @InjectBundleContext
    BundleContext context;
    
    @InjectService
    TypedEventBus eventBus;
    
    @Mock
    TestEventConsumer typedEventHandler, typedEventHandlerB;

    private AutoCloseable mocks;
    
    @BeforeEach
    public void setupMocks() {
        mocks = MockitoAnnotations.openMocks(this);
    }
    
    @AfterEach
    public void stop() throws Exception {
        mocks.close();
    }
    
    @Test
    public void testFilteredListener() throws Exception {
        Dictionary<String, Object> props = new Hashtable<>();
        props.put("event.filter", "(message=foo)");
        
        regs.add(context.registerService(TypedEventHandler.class, typedEventHandler, props));
        
        props = new Hashtable<>();
        props.put("event.filter", "(message=bar)");
        
        regs.add(context.registerService(TypedEventHandler.class, typedEventHandlerB, props));
        
        TestEvent event = new TestEvent();
        event.message = "foo";
        
        eventBus.deliver(event);
        
        Mockito.verify(typedEventHandler, Mockito.timeout(1000))
            .notify(Mockito.eq(TEST_EVENT_TOPIC), Mockito.argThat(isTestEventWithMessage("foo")));

        Mockito.verify(typedEventHandlerB, Mockito.after(1000).never())
            .notify(Mockito.eq(TEST_EVENT_TOPIC), Mockito.argThat(isTestEventWithMessage("foo")));
        
        
        event = new TestEvent();
        event.message = "bar";
        
        eventBus.deliver(event);
        
        Mockito.verify(typedEventHandlerB, Mockito.timeout(1000))
            .notify(Mockito.eq(TEST_EVENT_TOPIC), Mockito.argThat(isTestEventWithMessage("bar")));

        Mockito.verify(typedEventHandler, Mockito.after(1000).never())
            .notify(Mockito.eq(TEST_EVENT_TOPIC), Mockito.argThat(isTestEventWithMessage("bar")));
    }

    @Test
    public void testFilteredListenerEmptyString() throws Exception {
        Dictionary<String, Object> props = new Hashtable<>();
        props.put("event.filter", "");
        
        
        regs.add(context.registerService(TypedEventHandler.class, typedEventHandler, props));
        
        TestEvent event = new TestEvent();
        event.message = "foo";
        
        eventBus.deliver(event);
        
        Mockito.verify(typedEventHandler, Mockito.timeout(1000))
            .notify(Mockito.eq(TEST_EVENT_TOPIC), Mockito.argThat(isTestEventWithMessage("foo")));
    }
    
}