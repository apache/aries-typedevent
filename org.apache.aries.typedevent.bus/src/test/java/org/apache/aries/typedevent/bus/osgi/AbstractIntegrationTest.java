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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.aries.typedevent.bus.common.TestEvent;
import org.apache.aries.typedevent.bus.common.TestEvent2;
import org.junit.jupiter.api.AfterEach;
import org.mockito.ArgumentMatcher;
import org.osgi.framework.ServiceRegistration;

/**
 * This is a JUnit test that will be run inside an OSGi framework.
 * 
 * It can interact with the framework by starting or stopping bundles,
 * getting or registering services, or in other ways, and then observing
 * the result on the bundle(s) being tested.
 */
public abstract class AbstractIntegrationTest {
    
    protected static final String TEST_EVENT_TOPIC = TestEvent.class.getName().replace(".", "/");

    protected static final String TEST_EVENT_2_TOPIC = TestEvent2.class.getName().replace(".", "/");
    
    protected final List<ServiceRegistration<?>> regs = new ArrayList<ServiceRegistration<?>>();
    
    @AfterEach
    public void tearDown() throws Exception {
        regs.forEach(sr -> {
            try {
                sr.unregister();
            } catch (Exception e) { }
        });
    }
    
    protected ArgumentMatcher<TestEvent> isTestEventWithMessage(String message) {
        return new ArgumentMatcher<TestEvent>() {
            
            @Override
            public boolean matches(TestEvent argument) {
                return message.equals(argument.message);
            }
        };
    }

    protected ArgumentMatcher<TestEvent2> isTestEvent2WithMessage(String message) {
        return new ArgumentMatcher<TestEvent2>() {
            
            @Override
            public boolean matches(TestEvent2 argument) {
                return message.equals(argument.subEvent.message);
            }
        };
    }
    
    protected ArgumentMatcher<Map<String, Object>> isUntypedTestEventWithMessage(String message) {
        return new ArgumentMatcher<Map<String, Object>>() {
            
            @Override
            public boolean matches(Map<String, Object> argument) {
                return argument != null && message.equals(argument.get("message"));
            }
        };
    }
}