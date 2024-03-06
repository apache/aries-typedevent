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

import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_FILTER;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_TOPICS;

import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.aries.typedevent.bus.common.TestEvent;
import org.apache.aries.typedevent.bus.common.TestEventConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
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
@ExtendWith(MockitoExtension.class)
public class RecordIntegrationTest extends AbstractIntegrationTest {
    
	private static final String TOPIC = "org/apache/aries/test/record";
	
    @InjectBundleContext
    BundleContext context;
    
    @InjectService
    TypedEventBus eventBus;
    
    @Mock
    TestEventConsumer typedEventHandler;

    @Mock
    TestRecordListener recordEventHandler;

    @Test
    public void testUnFilteredListenerEventToRecord() throws Exception {
    	Dictionary<String, Object> props = new Hashtable<>();
    	props.put(TYPED_EVENT_TOPICS, TOPIC);
    	
    	regs.add(context.registerService(TypedEventHandler.class, typedEventHandler, props));
    	
    	props = new Hashtable<>();
    	props.put(TYPED_EVENT_TOPICS, TOPIC);
    	
    	regs.add(context.registerService(TypedEventHandler.class, recordEventHandler, props));
    	
    	// Event to record
    	
    	TestEvent event = new TestEvent();
    	event.message = "foo";
    	
    	eventBus.deliver(TOPIC, event);
    	
    	Mockito.verify(typedEventHandler, Mockito.timeout(1000))
    	.notify(Mockito.eq(TOPIC), Mockito.argThat(isTestEventWithMessage("foo")));
    	
    	Mockito.verify(recordEventHandler, Mockito.timeout(1000))
    	.notify(Mockito.eq(TOPIC), Mockito.argThat(isTestRecordWithMessage("foo")));
    }

    @Test
    public void testUnFilteredListenerRecordToEvent() throws Exception {
    	Dictionary<String, Object> props = new Hashtable<>();
    	props.put(TYPED_EVENT_TOPICS, TOPIC);
    	
    	regs.add(context.registerService(TypedEventHandler.class, typedEventHandler, props));
    	
    	props = new Hashtable<>();
    	props.put(TYPED_EVENT_TOPICS, TOPIC);
    	
    	regs.add(context.registerService(TypedEventHandler.class, recordEventHandler, props));
    	
    	// Record to Event
    	TestRecord testRecord = new TestRecord("bar");
    	
    	eventBus.deliver(TOPIC, testRecord);
    	
    	Mockito.verify(typedEventHandler, Mockito.timeout(1000))
    	.notify(Mockito.eq(TOPIC), Mockito.argThat(isTestEventWithMessage("bar")));
    	
    	Mockito.verify(recordEventHandler, Mockito.timeout(1000))
    	.notify(Mockito.eq(TOPIC), Mockito.argThat(isTestRecordWithMessage("bar")));
    	
    }

    @Test
    public void testUnFilteredListenerRecordToRecord() throws Exception {
    	Dictionary<String, Object> props = new Hashtable<>();
    	props.put(TYPED_EVENT_TOPICS, TOPIC);
    	
    	regs.add(context.registerService(TypedEventHandler.class, typedEventHandler, props));
    	
    	props = new Hashtable<>();
    	props.put(TYPED_EVENT_TOPICS, TOPIC);
    	
    	regs.add(context.registerService(TypedEventHandler.class, recordEventHandler, props));
    	
    	// Record to Record
    	TestRecord2 testRecord2 = new TestRecord2("foobar", 5);
    	
    	eventBus.deliver(TOPIC, testRecord2);
    	
    	Mockito.verify(typedEventHandler, Mockito.timeout(1000))
    	.notify(Mockito.eq(TOPIC), Mockito.argThat(isTestEventWithMessage("foobar")));
    	
    	Mockito.verify(recordEventHandler, Mockito.timeout(1000))
    	.notify(Mockito.eq(TOPIC), Mockito.argThat(isTestRecordWithMessage("foobar")));
    	
    }

    @Test
    public void testFilteredListenerEventToRecord() throws Exception {
        Dictionary<String, Object> props = new Hashtable<>();
        props.put(TYPED_EVENT_FILTER, "(message=foo)");
        props.put(TYPED_EVENT_TOPICS, TOPIC);
        
        regs.add(context.registerService(TypedEventHandler.class, typedEventHandler, props));
        
        props = new Hashtable<>();
        props.put(TYPED_EVENT_FILTER, "(message=bar)");
        props.put(TYPED_EVENT_TOPICS, TOPIC);
        
        regs.add(context.registerService(TypedEventHandler.class, recordEventHandler, props));
        
        // Event to record
        
        TestEvent event = new TestEvent();
        event.message = "foo";
        
        eventBus.deliver(TOPIC, event);
        
        Mockito.verify(typedEventHandler, Mockito.timeout(1000))
            .notify(Mockito.eq(TOPIC), Mockito.argThat(isTestEventWithMessage("foo")));

        Mockito.verify(recordEventHandler, Mockito.after(1000).never())
            .notify(Mockito.eq(TOPIC), Mockito.argThat(isTestRecordWithMessage("foo")));
        
        
        event = new TestEvent();
        event.message = "bar";
        
        eventBus.deliver(TOPIC, event);
        
        Mockito.verify(recordEventHandler, Mockito.timeout(1000))
            .notify(Mockito.eq(TOPIC), Mockito.argThat(isTestRecordWithMessage("bar")));

        Mockito.verify(typedEventHandler, Mockito.after(1000).never())
            .notify(Mockito.eq(TOPIC), Mockito.argThat(isTestEventWithMessage("bar")));
    }

    @Test
    public void testFilteredListenerRecordToEvent() throws Exception {
    	Dictionary<String, Object> props = new Hashtable<>();
    	props.put(TYPED_EVENT_FILTER, "(message=foo)");
    	props.put(TYPED_EVENT_TOPICS, TOPIC);
    	
    	regs.add(context.registerService(TypedEventHandler.class, typedEventHandler, props));
    	
    	props = new Hashtable<>();
    	props.put(TYPED_EVENT_FILTER, "(message=bar)");
    	props.put(TYPED_EVENT_TOPICS, TOPIC);
    	
    	regs.add(context.registerService(TypedEventHandler.class, recordEventHandler, props));
    	
    	// Record to Event
    	TestRecord testRecord = new TestRecord("foo");
    	
    	eventBus.deliver(TOPIC, testRecord);
    	
    	Mockito.verify(typedEventHandler, Mockito.timeout(1000))
    	.notify(Mockito.eq(TOPIC), Mockito.argThat(isTestEventWithMessage("foo")));
    	
    	Mockito.verify(recordEventHandler, Mockito.after(1000).never())
    	.notify(Mockito.eq(TOPIC), Mockito.argThat(isTestRecordWithMessage("foo")));
    	
    	
    	testRecord = new TestRecord("bar");
    	
    	eventBus.deliver(TOPIC, testRecord);
    	
    	Mockito.verify(recordEventHandler, Mockito.timeout(1000))
    	.notify(Mockito.eq(TOPIC), Mockito.argThat(isTestRecordWithMessage("bar")));
    	
    	Mockito.verify(typedEventHandler, Mockito.after(1000).never())
    	.notify(Mockito.eq(TOPIC), Mockito.argThat(isTestEventWithMessage("bar")));
    }

    @Test
    public void testFilteredListenerRecordToRecord() throws Exception {
    	Dictionary<String, Object> props = new Hashtable<>();
    	props.put(TYPED_EVENT_FILTER, "(message=foo)");
    	props.put(TYPED_EVENT_TOPICS, TOPIC);
    	
    	regs.add(context.registerService(TypedEventHandler.class, typedEventHandler, props));
    	
    	props = new Hashtable<>();
    	props.put(TYPED_EVENT_FILTER, "(message=bar)");
    	props.put(TYPED_EVENT_TOPICS, TOPIC);
    	
    	regs.add(context.registerService(TypedEventHandler.class, recordEventHandler, props));
    	
    	// Record to Record
    	TestRecord2 testRecord2 = new TestRecord2("foo", 5);
    	
    	eventBus.deliver(TOPIC, testRecord2);
    	
    	Mockito.verify(typedEventHandler, Mockito.timeout(1000))
    	.notify(Mockito.eq(TOPIC), Mockito.argThat(isTestEventWithMessage("foo")));
    	
    	Mockito.verify(recordEventHandler, Mockito.after(1000).never())
    	.notify(Mockito.eq(TOPIC), Mockito.argThat(isTestRecordWithMessage("foo")));
    	
    	
    	testRecord2 = new TestRecord2("bar", 5);
    	
    	eventBus.deliver(TOPIC, testRecord2);
    	
    	Mockito.verify(recordEventHandler, Mockito.timeout(1000))
    	.notify(Mockito.eq(TOPIC), Mockito.argThat(isTestRecordWithMessage("bar")));
    	
    	Mockito.verify(typedEventHandler, Mockito.after(1000).never())
    	.notify(Mockito.eq(TOPIC), Mockito.argThat(isTestEventWithMessage("bar")));
    }
    
    public interface TestRecordListener extends TypedEventHandler<TestRecord> {}
    
    public record TestRecord(String message) {}
    
    public record TestRecord2(String message, int count) {}
    
    protected ArgumentMatcher<TestRecord> isTestRecordWithMessage(String message) {
        return new ArgumentMatcher<TestRecord>() {
            
            @Override
            public boolean matches(TestRecord argument) {
                return message.equals(argument.message);
            }
        };
    }
}