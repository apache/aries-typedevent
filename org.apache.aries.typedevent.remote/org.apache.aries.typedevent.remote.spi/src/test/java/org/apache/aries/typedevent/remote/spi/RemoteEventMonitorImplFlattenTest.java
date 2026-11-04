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
package org.apache.aries.typedevent.remote.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;

import org.junit.jupiter.api.Test;

public class RemoteEventMonitorImplFlattenTest {

    @Test
    public void testNestedMapsAreFlattened() {
        Map<String, Object> inner = new HashMap<>();
        inner.put("x", 1);
        Map<String, Object> eventData = new LinkedHashMap<>();
        eventData.put("a", "top");
        eventData.put("b", inner);
        eventData.put("c", null);

        SortedMap<String, Object> flat = RemoteEventMonitorImpl.flatten(eventData);

        assertEquals(3, flat.size());
        assertEquals("top", flat.get("a"));
        assertEquals(1, flat.get("b.x"));
        assertEquals(null, flat.get("c"));
    }

    @Test
    public void testMapReferringBackToItselfTerminates() {
        Map<String, Object> eventData = new LinkedHashMap<>();
        eventData.put("a", "top");
        eventData.put("self", eventData);

        SortedMap<String, Object> flat = RemoteEventMonitorImpl.flatten(eventData);

        assertEquals(1, flat.size());
        assertEquals("top", flat.get("a"));
    }

    @Test
    public void testCycleBetweenNestedMapsTerminates() {
        Map<String, Object> first = new LinkedHashMap<>();
        Map<String, Object> second = new LinkedHashMap<>();
        first.put("name", "first");
        first.put("next", second);
        second.put("name", "second");
        second.put("next", first);

        Map<String, Object> eventData = new LinkedHashMap<>();
        eventData.put("start", first);

        SortedMap<String, Object> flat = RemoteEventMonitorImpl.flatten(eventData);

        assertEquals(2, flat.size());
        assertEquals("first", flat.get("start.name"));
        assertEquals("second", flat.get("start.next.name"));
    }

    @Test
    public void testSharedMapIsNotACycle() {
        Map<String, Object> shared = new HashMap<>();
        shared.put("x", 1);
        Map<String, Object> eventData = new LinkedHashMap<>();
        eventData.put("first", shared);
        eventData.put("second", shared);

        SortedMap<String, Object> flat = RemoteEventMonitorImpl.flatten(eventData);

        assertEquals(2, flat.size());
        assertEquals(1, flat.get("first.x"));
        assertEquals(1, flat.get("second.x"));
    }
}
