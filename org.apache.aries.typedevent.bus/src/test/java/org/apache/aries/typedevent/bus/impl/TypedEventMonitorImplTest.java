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

import java.util.Collections;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.osgi.service.typedevent.monitor.RangePolicy;

@ExtendWith(MockitoExtension.class)
class TypedEventMonitorImplTest {

	private static final String TOPIC = "a/b/c";
	TypedEventMonitorImpl monitorImpl;

	@BeforeEach
	void start() {
		monitorImpl = new TypedEventMonitorImpl(Collections.emptyMap());
	}

	@AfterEach
	void stop() {
		monitorImpl.destroy();
	}

	/**
	 * Tests history storage size configuration
	 */
	@Test
	void testConfigureHistoryStorage() {
		checkRangeConfiguration(RangePolicy.unlimited());
		checkRangeConfiguration(RangePolicy.atLeast(0));
		checkRangeConfiguration(RangePolicy.range(0, Integer.MAX_VALUE));
		checkRangeConfiguration(RangePolicy.atLeast(10));
		checkRangeConfiguration(RangePolicy.range(1, Integer.MAX_VALUE));
	}

	private void checkRangeConfiguration(final RangePolicy policy) {
		monitorImpl.configureHistoryStorage(TOPIC, policy);
		RangePolicy configured = monitorImpl.getConfiguredHistoryStorage(TOPIC);
		assertEquals(policy.getMinimum(), configured.getMinimum());
		assertEquals(policy.getMaximum(), configured.getMaximum());
		RangePolicy effective = monitorImpl.getEffectiveHistoryStorage(TOPIC);
		assertEquals(effective.getMinimum(), effective.getMinimum());
		assertEquals(policy.getMaximum(), effective.getMaximum());
	}
}
