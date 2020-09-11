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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.osgi.util.converter.ConversionException;

public class EventConverterTest {

    public static class TestEvent {
        public String message;
    }

    public static class NestedEventHolder {
        public TestEvent event;
    }

    static class DefaultVisibilityNestedEventHolder {
        public TestEvent event;
    }

    public static class NestedEventHolderNotAProperDTO {
        public TestEvent event;

        public static NestedEventHolderNotAProperDTO factory(TestEvent event) {
            NestedEventHolderNotAProperDTO holder = new NestedEventHolderNotAProperDTO();
            holder.event = event;
            return holder;
        }
    }

    public static class DoublyNestedEventHolderWithIssues {
        public NestedEventHolderNotAProperDTO event;
    }

    @Test
    public void testSimpleFlattenAndReconstitute() {

        TestEvent te = new TestEvent();
        te.message = "FOO";

        Map<String, ?> map = EventConverter.forTypedEvent(te).toUntypedEvent();

        assertEquals("FOO", map.get("message"));

        TestEvent testEvent = EventConverter.forUntypedEvent(map).toTypedEvent(TestEvent.class);

        assertEquals(te.message, testEvent.message);

    }

    @Test
    public void testNestedFlattenAndReconstitute() {

        TestEvent te = new TestEvent();
        te.message = "FOO";

        NestedEventHolder holder = new NestedEventHolder();
        holder.event = te;

        Map<String, ?> map = EventConverter.forTypedEvent(holder).toUntypedEvent();

        @SuppressWarnings({ "unchecked" })
        Map<String, Object> nested = (Map<String, Object>) map.get("event");
        assertEquals("FOO", nested.get("message"));

        NestedEventHolder testEvent = EventConverter.forUntypedEvent(map).toTypedEvent(NestedEventHolder.class);

        assertEquals(te.message, testEvent.event.message);

    }

    @Test
    public void testDefaultVisibiltyNestedFlattenAndReconstitute() {

        TestEvent te = new TestEvent();
        te.message = "FOO";

        NestedEventHolder holder = new NestedEventHolder();
        holder.event = te;

        Map<String, ?> map = EventConverter.forTypedEvent(holder).toUntypedEvent();

        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) map.get("event");
        assertEquals("FOO", nested.get("message"));

        try {
            EventConverter.forUntypedEvent(map).toTypedEvent(DefaultVisibilityNestedEventHolder.class);
            fail("Should not succeed in creating a Default Visibility type");
        } catch (ConversionException ce) {
            assertEquals(IllegalAccessException.class, ce.getCause().getClass());
        }
    }

    @Test
    public void testNestedFlattenAndReconstituteNotAProperDTO() {

        TestEvent te = new TestEvent();
        te.message = "FOO";

        NestedEventHolderNotAProperDTO holder = new NestedEventHolderNotAProperDTO();
        holder.event = te;

        Map<String, ?> map = EventConverter.forTypedEvent(holder).toUntypedEvent();

        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) map.get("event");
        assertEquals("FOO", nested.get("message"));

        NestedEventHolderNotAProperDTO testEvent = EventConverter.forUntypedEvent(map)
                .toTypedEvent(NestedEventHolderNotAProperDTO.class);

        assertEquals(te.message, testEvent.event.message);

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDoublyNestedFlattenAndReconstitute() {

        TestEvent te = new TestEvent();
        te.message = "FOO";

        NestedEventHolderNotAProperDTO holder = new NestedEventHolderNotAProperDTO();
        holder.event = te;

        DoublyNestedEventHolderWithIssues doubleHolder = new DoublyNestedEventHolderWithIssues();
        doubleHolder.event = holder;

        Map<String, ?> map = EventConverter.forTypedEvent(doubleHolder).toUntypedEvent();

        Map<String, Object> nested = (Map<String, Object>) map.get("event");
        assertTrue(nested.containsKey("event"));
        nested = (Map<String, Object>) nested.get("event");
        assertEquals("FOO", nested.get("message"));

        DoublyNestedEventHolderWithIssues testEvent = EventConverter.forUntypedEvent(map)
                .toTypedEvent(DoublyNestedEventHolderWithIssues.class);

        assertEquals(te.message, testEvent.event.event.message);

    }

    @Test
    public void testNestedFlattenAndReconstituteIntoADifferentType() {

        TestEvent te = new TestEvent();
        te.message = "FOO";

        NestedEventHolder holder = new NestedEventHolder();
        holder.event = te;

        Map<String, ?> map = EventConverter.forTypedEvent(holder).toUntypedEvent();

        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) map.get("event");
        assertEquals("FOO", nested.get("message"));

        NestedEventHolderNotAProperDTO testEvent = EventConverter.forUntypedEvent(map)
                .toTypedEvent(NestedEventHolderNotAProperDTO.class);

        assertEquals(te.message, testEvent.event.message);

    }

}
