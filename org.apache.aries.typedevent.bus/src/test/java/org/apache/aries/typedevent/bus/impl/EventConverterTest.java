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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

import org.apache.aries.typedevent.bus.spi.CustomEventConverter;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.osgi.service.typedevent.TypedEventHandler;
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
    
    public static class ParameterizedEvent<T> {
    	public T parameterisedMessage;
    }

    public static interface IntegerTestHandler extends TypedEventHandler<ParameterizedEvent<Integer>> {}

    public static interface DoubleTestHandler extends TypedEventHandler<ParameterizedEvent<Double>> {}
    
    public static class DoublyNestedEventHolderWithIssues {
        public NestedEventHolderNotAProperDTO event;
    }

    @Test
    public void testSimpleFlattenAndReconstitute() {

        TestEvent te = new TestEvent();
        te.message = "FOO";

        Map<String, ?> map = EventConverter.forTypedEvent(te, null).toUntypedEvent();

        assertEquals("FOO", map.get("message"));

        TestEvent testEvent = EventConverter.forUntypedEvent(map, null)
        		.toTypedEvent(new TypeData(TestEvent.class));

        assertEquals(te.message, testEvent.message);

    }

    @Test
    public void testNestedFlattenAndReconstitute() {

        TestEvent te = new TestEvent();
        te.message = "FOO";

        NestedEventHolder holder = new NestedEventHolder();
        holder.event = te;

        Map<String, ?> map = EventConverter.forTypedEvent(holder, null).toUntypedEvent();

        @SuppressWarnings({ "unchecked" })
        Map<String, Object> nested = (Map<String, Object>) map.get("event");
        assertEquals("FOO", nested.get("message"));

        NestedEventHolder testEvent = EventConverter.forUntypedEvent(map, null)
        		.toTypedEvent(new TypeData(NestedEventHolder.class));

        assertEquals(te.message, testEvent.event.message);

    }

    @Test
    public void testDefaultVisibiltyNestedFlattenAndReconstitute() {

        TestEvent te = new TestEvent();
        te.message = "FOO";

        NestedEventHolder holder = new NestedEventHolder();
        holder.event = te;

        Map<String, ?> map = EventConverter.forTypedEvent(holder, null).toUntypedEvent();

        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) map.get("event");
        assertEquals("FOO", nested.get("message"));

        try {
            EventConverter.forUntypedEvent(map, null).toTypedEvent(
            		new TypeData(DefaultVisibilityNestedEventHolder.class));
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

        Map<String, ?> map = EventConverter.forTypedEvent(holder, null).toUntypedEvent();

        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) map.get("event");
        assertEquals("FOO", nested.get("message"));

        NestedEventHolderNotAProperDTO testEvent = EventConverter.forUntypedEvent(map, null)
                .toTypedEvent(new TypeData(NestedEventHolderNotAProperDTO.class));

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

        Map<String, ?> map = EventConverter.forTypedEvent(doubleHolder, null).toUntypedEvent();

        Map<String, Object> nested = (Map<String, Object>) map.get("event");
        assertTrue(nested.containsKey("event"));
        nested = (Map<String, Object>) nested.get("event");
        assertEquals("FOO", nested.get("message"));

        DoublyNestedEventHolderWithIssues testEvent = EventConverter.forUntypedEvent(map, null)
                .toTypedEvent(new TypeData(DoublyNestedEventHolderWithIssues.class));

        assertEquals(te.message, testEvent.event.event.message);

    }

    @Test
    public void testNestedFlattenAndReconstituteIntoADifferentType() {

        TestEvent te = new TestEvent();
        te.message = "FOO";

        NestedEventHolder holder = new NestedEventHolder();
        holder.event = te;

        Map<String, ?> map = EventConverter.forTypedEvent(holder, null).toUntypedEvent();

        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) map.get("event");
        assertEquals("FOO", nested.get("message"));

        NestedEventHolderNotAProperDTO testEvent = EventConverter.forUntypedEvent(map, null)
                .toTypedEvent(new TypeData(NestedEventHolderNotAProperDTO.class));

        assertEquals(te.message, testEvent.event.message);

    }

    @Test
    public void testGenericFlattenAndReconstituteIntoADifferentType() {
    	
    	ParameterizedEvent<Integer> te = new ParameterizedEvent<>();
    	te.parameterisedMessage = 42;
    	
    	Type integerType = ((ParameterizedType)IntegerTestHandler.class.getGenericInterfaces()[0])
    			.getActualTypeArguments()[0];
    	Type doubleType = ((ParameterizedType)DoubleTestHandler.class.getGenericInterfaces()[0])
    			.getActualTypeArguments()[0];
    	
    	EventConverter eventConverter = EventConverter.forTypedEvent(te, null);
		
    	Map<String, ?> map = eventConverter.toUntypedEvent();
    	assertEquals(42, map.get("parameterisedMessage"));
    	
    	ParameterizedEvent<Double> converted = eventConverter.toTypedEvent(new TypeData(doubleType));
    	assertEquals(42d, converted.parameterisedMessage, 0.00001);
    	
    	ParameterizedEvent<Integer> testEvent = EventConverter.forUntypedEvent(
    			Map.of("parameterisedMessage", "17"), null)
    			.toTypedEvent(new TypeData(integerType));
    	
    	assertEquals(17, testEvent.parameterisedMessage);
    	
    }

    @Test
    public void testCustomConverterRawTypes() {
    	
    	TestEvent te = new TestEvent();
        te.message = "FOO";
        CustomEventConverter cec = Mockito.mock(CustomEventConverter.class);
        
        Mockito.when(cec.toUntypedEvent(te)).thenReturn(Map.of("message", "BAR"));
        Mockito.when(cec.toTypedEvent(te, String.class, String.class)).thenReturn("FOOBAR");
        Mockito.when(cec.toTypedEvent(te, TestEvent.class, TestEvent.class)).thenReturn("FIZZBUZZ");

        EventConverter eventConverter = EventConverter.forTypedEvent(te, cec);
		
        Map<String, ?> map = eventConverter.toUntypedEvent();
        assertEquals("BAR", map.get("message"));
        assertEquals("FOOBAR", eventConverter.toTypedEvent(new TypeData(String.class)));

        // Bypass the conversion if identity
        assertSame(te, eventConverter.toTypedEvent(new TypeData(TestEvent.class)));
        
    }
    
    @Test
    public void testCustomConverterWithGenerics() {
    	
    	ParameterizedEvent<Integer> te = new ParameterizedEvent<>();
    	te.parameterisedMessage = 42;
    	
    	Type integerType = ((ParameterizedType)IntegerTestHandler.class.getGenericInterfaces()[0])
    			.getActualTypeArguments()[0];
    	Type doubleType = ((ParameterizedType)DoubleTestHandler.class.getGenericInterfaces()[0])
    			.getActualTypeArguments()[0];
    	
    	CustomEventConverter cec = Mockito.mock(CustomEventConverter.class);
    	
    	Mockito.when(cec.toUntypedEvent(te)).thenReturn(Map.of("parameterisedMessage", "21"));
        Mockito.when(cec.toTypedEvent(te, ParameterizedEvent.class, integerType)).thenReturn(21d);
        Mockito.when(cec.toTypedEvent(te, ParameterizedEvent.class, doubleType)).thenReturn(63d);
    	
    	EventConverter eventConverter = EventConverter.forTypedEvent(te, cec);
    	
    	Map<String, ?> map = eventConverter.toUntypedEvent();
    	assertEquals("21", map.get("parameterisedMessage"));
    	
    	// Never bypass as we can't easily verify the generics
    	assertEquals(21d, eventConverter.toTypedEvent(new TypeData(integerType)), 0.00001);
    	assertEquals(63d, eventConverter.toTypedEvent(new TypeData(doubleType)), 0.00001);
    	
    }

}
