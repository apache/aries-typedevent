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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.aries.typedevent.bus.spi.CustomEventConverter;
import org.apache.aries.typedevent.bus.spi.TypeData;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.osgi.framework.Filter;
import org.osgi.util.converter.ConversionException;

/**
 * Events that are not trees, because they contain a cycle, must not break the
 * event bus
 */
public class EventConverterCycleTest {

    public static class CyclicEvent {
        public String name;
        public CyclicEvent other;
        public List<Object> list;
        public Set<Object> set;
        public Map<String, Object> map;
        public Object[] array;
    }

    public static class OtherCyclicEvent {
        public String name;
        public OtherCyclicEvent other;
    }

    public static class Inner {
        public String message;
    }

    public static class SharedEvent {
        public Inner first;
        public Inner second;
        public List<Inner> list;
    }

    public static class InnerCopy {
        public String message;
    }

    public static class SharedEventCopy {
        public InnerCopy first;
        public InnerCopy second;
        public List<InnerCopy> list;
    }

    private static CyclicEvent event(String name) {
        CyclicEvent event = new CyclicEvent();
        event.name = name;
        return event;
    }

    private static TypeData typeData(Class<?> type) {
        return new TypeData(type, Collections.emptyMap());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        return (Map<String, Object>) o;
    }

    @Test
    public void testSelfReference() {
        CyclicEvent a = event("a");
        a.other = a;

        Map<String, Object> flat = EventConverter.forTypedEvent(a, null).toUntypedEvent();

        assertEquals("a", flat.get("name"));
        assertSame(flat, flat.get("other"));
    }

    @Test
    public void testTwoObjectCycle() {
        CyclicEvent a = event("a");
        CyclicEvent b = event("b");
        a.other = b;
        b.other = a;

        Map<String, Object> flat = EventConverter.forTypedEvent(a, null).toUntypedEvent();

        Map<String, Object> flatB = asMap(flat.get("other"));
        assertEquals("b", flatB.get("name"));
        assertSame(flat, flatB.get("other"));
    }

    @Test
    public void testCycleThroughList() {
        CyclicEvent a = event("a");
        a.list = new ArrayList<>();
        a.list.add(a);
        a.list.add("end");

        Map<String, Object> flat = EventConverter.forTypedEvent(a, null).toUntypedEvent();

        List<?> flatList = (List<?>) flat.get("list");
        assertSame(flat, flatList.get(0));
        assertEquals("end", flatList.get(1));
    }

    @Test
    public void testCycleThroughSet() {
        CyclicEvent a = event("a");
        a.set = new LinkedHashSet<>();
        a.set.add(a);

        Map<String, Object> flat = EventConverter.forTypedEvent(a, null).toUntypedEvent();

        Set<?> flatSet = (Set<?>) flat.get("set");
        assertEquals(1, flatSet.size());
        assertSame(flat, flatSet.iterator().next());
    }

    @Test
    public void testCycleThroughMap() {
        CyclicEvent a = event("a");
        a.map = new HashMap<>();
        a.map.put("self", a);

        Map<String, Object> flat = EventConverter.forTypedEvent(a, null).toUntypedEvent();

        assertSame(flat, asMap(flat.get("map")).get("self"));
    }

    @Test
    public void testCycleThroughArray() {
        CyclicEvent a = event("a");
        a.array = new Object[] { a, "end" };

        Map<String, Object> flat = EventConverter.forTypedEvent(a, null).toUntypedEvent();

        Object[] flatArray = (Object[]) flat.get("array");
        assertSame(flat, flatArray[0]);
        assertEquals("end", flatArray[1]);
    }

    @Test
    public void testCycleNotInvolvingTheRoot() {
        CyclicEvent a = event("a");
        CyclicEvent b = event("b");
        CyclicEvent c = event("c");
        a.other = b;
        b.other = c;
        c.other = b;

        Map<String, Object> flat = EventConverter.forTypedEvent(a, null).toUntypedEvent();

        Map<String, Object> flatB = asMap(flat.get("other"));
        Map<String, Object> flatC = asMap(flatB.get("other"));
        assertEquals("c", flatC.get("name"));
        assertSame(flatB, flatC.get("other"));
    }

    @Test
    public void testUntypedFormIsOnlyCreatedOnce() {
        CyclicEvent a = event("a");
        a.other = a;

        EventConverter converter = EventConverter.forTypedEvent(a, null);

        assertSame(converter.toUntypedEvent(), converter.toUntypedEvent());
    }

    @Test
    public void testConcurrentFlattening() throws Exception {
        CyclicEvent a = event("a");
        a.other = a;

        EventConverter converter = EventConverter.forTypedEvent(a, null);

        int threads = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Map<String, Object>>> results = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                results.add(executor.submit(() -> {
                    start.await();
                    return converter.toUntypedEvent();
                }));
            }
            start.countDown();

            Map<Object, Boolean> distinct = new IdentityHashMap<>();
            for (Future<Map<String, Object>> result : results) {
                distinct.put(result.get(10, TimeUnit.SECONDS), Boolean.TRUE);
            }
            assertEquals(1, distinct.size());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSharedObjectIsNotACycle() {
        Inner inner = new Inner();
        inner.message = "shared";
        SharedEvent shared = new SharedEvent();
        shared.first = inner;
        shared.second = inner;
        shared.list = new ArrayList<>();
        shared.list.add(inner);

        EventConverter converter = EventConverter.forTypedEvent(shared, null);
        Map<String, Object> flat = converter.toUntypedEvent();

        assertEquals("shared", asMap(flat.get("first")).get("message"));
        assertSame(flat.get("first"), flat.get("second"));

        // A tree-like event can still be converted to another type
        SharedEventCopy copy = converter.toTypedEvent(typeData(SharedEventCopy.class));
        assertEquals("shared", copy.first.message);
        assertEquals("shared", copy.second.message);
        assertEquals("shared", copy.list.get(0).message);
    }

    @Test
    public void testNestedCollectionsAreStillCopied() {
        CyclicEvent a = event("a");
        a.list = new ArrayList<>();
        a.list.add(event("in list"));

        Map<String, Object> flat = EventConverter.forTypedEvent(a, null).toUntypedEvent();

        Object element = ((List<?>) flat.get("list")).get(0);
        assertEquals("in list", asMap(element).get("name"));
        assertNotSame(a.list.get(0), element);
    }

    @Test
    public void testTypedHandlerOfTheSameTypeKeepsTheCycle() {
        CyclicEvent a = event("a");
        a.other = a;

        EventConverter converter = EventConverter.forTypedEvent(a, null);
        // Flattening first must make no difference
        converter.toUntypedEvent();

        CyclicEvent delivered = converter.toTypedEvent(typeData(CyclicEvent.class));

        assertSame(a, delivered);
        assertSame(delivered, delivered.other);
    }

    @Test
    public void testConversionOfCycleToOtherTypeFailsFastOnceFlattened() {
        CyclicEvent a = event("a");
        a.other = a;

        EventConverter converter = EventConverter.forTypedEvent(a, null);
        converter.toUntypedEvent();

        assertThrows(IllegalArgumentException.class, () -> converter.toTypedEvent(typeData(OtherCyclicEvent.class)));
        assertThrows(IllegalArgumentException.class, () -> converter.toTypedEvent(typeData(OtherCyclicEvent.class)));
    }

    @Test
    public void testConversionOfCycleToOtherTypeFailsCleanly() {
        CyclicEvent a = event("a");
        a.other = a;

        EventConverter converter = EventConverter.forTypedEvent(a, null);

        assertThrows(IllegalArgumentException.class, () -> converter.toTypedEvent(typeData(OtherCyclicEvent.class)));
        // The failure is remembered, and the event can still be flattened
        assertThrows(IllegalArgumentException.class, () -> converter.toTypedEvent(typeData(OtherCyclicEvent.class)));
        assertSame(converter.toUntypedEvent(), converter.toUntypedEvent());
    }

    @Test
    public void testFailedFlatteningIsNotRepeated() {
        CyclicEvent a = event("a");
        CustomEventConverter custom = Mockito.mock(CustomEventConverter.class);
        Mockito.when(custom.toUntypedEvent(a)).thenThrow(new IllegalStateException("Cannot flatten"));

        EventConverter converter = EventConverter.forTypedEvent(a, custom);

        ConversionException first = assertThrows(ConversionException.class, converter::toUntypedEvent);
        ConversionException second = assertThrows(ConversionException.class, converter::toUntypedEvent);

        assertSame(first, second);
        assertEquals(IllegalStateException.class, first.getCause().getClass());
        Mockito.verify(custom, Mockito.times(1)).toUntypedEvent(a);
    }

    @Test
    public void testFilterDoesNotMatchAnEventWithNoUntypedForm() {
        CyclicEvent a = event("a");
        CustomEventConverter custom = Mockito.mock(CustomEventConverter.class);
        Mockito.when(custom.toUntypedEvent(a)).thenThrow(new IllegalStateException("Cannot flatten"));
        Filter filter = Mockito.mock(Filter.class);
        Mockito.when(filter.matches(Mockito.anyMap())).thenReturn(true);

        EventConverter converter = EventConverter.forTypedEvent(a, custom);

        assertFalse(converter.applyFilter(filter));
        Mockito.verifyNoInteractions(filter);
    }

    @Test
    public void testFilterMatchesAFlattenedCycle() {
        CyclicEvent a = event("a");
        a.other = a;
        Filter filter = Mockito.mock(Filter.class);
        Mockito.when(filter.matches(Mockito.anyMap())).thenReturn(true);

        EventConverter converter = EventConverter.forTypedEvent(a, null);

        assertTrue(converter.applyFilter(filter));
    }
}
