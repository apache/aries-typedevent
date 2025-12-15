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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.osgi.service.typedevent.monitor.RangePolicy;

@ExtendWith(MockitoExtension.class)
public class TypedEventMonitorImplTest {

    private static final String TOPIC_PATTERN_MULTI = "a/*";
    private static final String TOPIC_PATTERN_SINGLE = "a/b/+";
    private static final String TOPIC = "a/b/c";
    TypedEventMonitorImpl monitorImpl;

    @BeforeEach
    public void start() throws ClassNotFoundException {
        Map<String, Object> config = Collections.emptyMap();
        
        monitorImpl = new TypedEventMonitorImpl(config);
    }
    
    @AfterEach
    public void stop() throws Exception {
        monitorImpl.destroy();
    }

    /**
     * Tests that history configuration works for topics
     * 
     * @throws InterruptedException
     */
    @Test
    public void testConfigureHistoryStorage() throws InterruptedException {
        checkRangeConfiguration(RangePolicy.unlimited());
        checkRangeConfiguration(RangePolicy.atLeast(0));
        checkRangeConfiguration(RangePolicy.range(0,Integer.MAX_VALUE));
        checkRangeConfiguration(RangePolicy.atLeast(10));
        checkRangeConfiguration(RangePolicy.range(1,Integer.MAX_VALUE));
    }
    
    /**
     * Tests that history configuration works for wildcard topics
     * 
     * @throws InterruptedException
     */
    @Test
    public void testConfigureHistoryStorageWildcards() throws InterruptedException {
    	// Multi-level
    	checkRangeConfiguration(TOPIC_PATTERN_MULTI, TOPIC, RangePolicy.unlimited());
    	checkRangeConfiguration(TOPIC_PATTERN_MULTI, TOPIC, RangePolicy.atMost(10));
    	// Not permitted 
    	assertThrows(IllegalArgumentException.class, 
    			() -> checkRangeConfiguration(TOPIC_PATTERN_MULTI, TOPIC, RangePolicy.atLeast(10)));
    	// Not permitted 
    	assertThrows(IllegalArgumentException.class, 
    			() -> checkRangeConfiguration(TOPIC_PATTERN_MULTI, TOPIC, RangePolicy.range(1,Integer.MAX_VALUE)));

    	// Single-level
    	checkRangeConfiguration(TOPIC_PATTERN_SINGLE, TOPIC, RangePolicy.unlimited());
    	checkRangeConfiguration(TOPIC_PATTERN_SINGLE, TOPIC, RangePolicy.atMost(10));
    	// Not permitted 
    	assertThrows(IllegalArgumentException.class, 
    			() -> checkRangeConfiguration(TOPIC_PATTERN_SINGLE, TOPIC, RangePolicy.atLeast(10)));
    	// Not permitted 
    	assertThrows(IllegalArgumentException.class, 
    			() -> checkRangeConfiguration(TOPIC_PATTERN_SINGLE, TOPIC, RangePolicy.range(1,Integer.MAX_VALUE)));
    }

    @Test
    public void testTooBig() {
    	int total = monitorImpl.getMaximumEventStorage();
    	assertThrows(IllegalStateException.class, 
    			() -> monitorImpl.configureHistoryStorage(TOPIC, RangePolicy.atLeast(total + 1)));

    	RangePolicy half = RangePolicy.atLeast(total / 2);
    	monitorImpl.configureHistoryStorage("a", half);
    	monitorImpl.configureHistoryStorage("a/b", half);
    	
    	assertThrows(IllegalStateException.class, 
    			() -> monitorImpl.configureHistoryStorage(TOPIC, half));
    }

    private void checkRangeConfiguration(final RangePolicy policy) {
    	checkRangeConfiguration(TOPIC, TOPIC, policy);
    }

    private void checkRangeConfiguration(String configTopic, String testTopic, final RangePolicy policy) {
        monitorImpl.configureHistoryStorage(configTopic, policy);
        RangePolicy configured = monitorImpl.getConfiguredHistoryStorage(testTopic);
        if(configTopic.equals(testTopic)) {
            assertEquals(policy.getMinimum(), configured.getMinimum());
            assertEquals(policy.getMaximum(), configured.getMaximum());
        } else {
        	assertNull(configured);
        }
        RangePolicy effective = monitorImpl.getEffectiveHistoryStorage(testTopic);
        assertEquals(policy.getMinimum(), effective.getMinimum());
        assertEquals(policy.getMaximum(), effective.getMaximum());
    }

}
