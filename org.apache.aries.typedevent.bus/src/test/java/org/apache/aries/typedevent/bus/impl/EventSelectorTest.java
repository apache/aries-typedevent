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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.aries.typedevent.bus.impl.EventConverterTest.NestedEventHolderNotAProperDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.osgi.framework.Filter;

@ExtendWith(MockitoExtension.class)
public class EventSelectorTest {

    @Mock
    Filter mockFilter;
    
    @Mock
    EventConverter eventConverter;

    public static class DoublyNestedEventHolderWithIssues {
        public NestedEventHolderNotAProperDTO event;
    }

    @ParameterizedTest
    @MethodSource("getTopicMatchingData")
    public void testTopicMatching(String topic, String topicFilter, boolean expectedResult) {
        assertEquals(expectedResult, new EventSelector(topicFilter, null).matches(topic, new HashMap<>()));
    }
    
    /**
     * Test topics and filters for matching checks
     * @return
     */
    static Stream<Arguments> getTopicMatchingData() {
    	return List.of(
    				// Basic
    				Arguments.of("foo", "foo", true),
    				Arguments.of("foo/bar", "foo", false),
    				Arguments.of("foo/bar", "foo/bar", true),
    				Arguments.of("foo/bark", "foo/bar", false),
    				Arguments.of("foo/bar", "foo/barb", false),
    				// Multi Level Wildcard
    				Arguments.of("foo", "*", true),
    				Arguments.of("foo", "foo/*", false),
    				Arguments.of("foo/bar", "*", true),
    				Arguments.of("foo/bar", "foo/*", true),
    				Arguments.of("foo/foobar", "foo/*", true),
    				Arguments.of("foo/bar/foobar", "foo/*", true),
    				Arguments.of("foo/bar/foobar", "foo/bar/*", true),
    				Arguments.of("foo/bark/foobar", "foo/bar/*", false),
    				// Single Level Wildcard
    				Arguments.of("foo", "+", true),
    				Arguments.of("foo", "foo/+", false),
    				Arguments.of("foo/bar", "+", false),
    				Arguments.of("foo/bar", "foo/+", true),
    				Arguments.of("foo/bar", "+/bar", true),
    				Arguments.of("foo/foobar", "foo/+", true),
    				Arguments.of("fool/foobar", "foo/+", false),
    				Arguments.of("foo/foobar", "+/+", true),
    				Arguments.of("foo/bar/foobar", "foo/+", false),
    				Arguments.of("foo/bar/foobar", "foo/+/foobar", true),
    				Arguments.of("foo/bar/foobark", "foo/+/foobar", false),
    				Arguments.of("foo/bar/foobar", "foo/+/+", true),
    				Arguments.of("foo/bar/foobar", "+/bar/+", true),
    				Arguments.of("foo/bark/foobar", "foo/bar/+", false),
    				// Mixture of wildcards
    				Arguments.of("foo", "+/*", false),
    				Arguments.of("foo/bar", "+/*", true),
    				Arguments.of("foo/bar/foobar", "+/*", true),
    				Arguments.of("foo/bar/foobar", "+/bar/*", true),
    				Arguments.of("foo/bar/foobar", "+/bar/foobar/*", false),
    				Arguments.of("foo/bar/foobar", "+/bar/+/*", false),
    				Arguments.of("foo/bar/foobar", "+/+/*", true),
    				Arguments.of("foo/bar/foobar/fizz", "+/bar/+/*", true),
    				Arguments.of("foo/bar/foobar/fizz", "foo/+/foobar/*", true),
    				Arguments.of("foo/bar/foobar/fizz", "foo/+/+/*", true),
    				Arguments.of("fool/bar/foobar/fizz", "foo/+/+/*", false)
    			).stream();
    }

   @Test
   public void testEventFilteringNoTopicMatch() {
	   assertFalse(new EventSelector("foo/bar", mockFilter).matches("fizz/buzz", eventConverter));
	   Mockito.verifyNoInteractions(mockFilter, eventConverter);
   }

   @Test
   public void testEventFilteringFilterMatch() {
	   Mockito.when(eventConverter.applyFilter(mockFilter)).thenReturn(true);
	   assertTrue(new EventSelector("foo/bar", mockFilter).matches("foo/bar", eventConverter));
	   Mockito.verify(eventConverter).applyFilter(mockFilter);
   }

   @Test
   public void testEventFilteringNoFilterMatch() {
	   Mockito.when(eventConverter.applyFilter(mockFilter)).thenReturn(false);
	   assertFalse(new EventSelector("foo/bar", mockFilter).matches("foo/bar", eventConverter));
	   Mockito.verify(eventConverter).applyFilter(mockFilter);
   }
   
   @Test
   public void testOrdering() {
	   List<String> filters = Arrays.asList("*", "+", "event/*", "event/+/data", "event/foo/data", "event/foo/+",
			   "event/bar/*", "event/foo/*", "event/+", "event/+/+", "event/+/*", "bigevent/+/*", "bigevent/foo/data");
	   
	   Map<String, EventSelector> selectors = filters.stream().collect(toMap(identity(), f -> new EventSelector(f, null)));
	   
	   // Work from the highest sort (lowest priority) to the beginning
	   doTest(selectors, "*", Arrays.asList("+", "event/*", "event/+/data", "event/foo/data", "event/foo/+",
			   "event/bar/*", "event/foo/*", "event/+", "event/+/+", "event/+/*", "bigevent/+/*", "bigevent/foo/data"),
			   Collections.emptyList());
	   doTest(selectors, "+", Arrays.asList("event/*", "event/+/data", "event/foo/data", "event/foo/+",
			   "event/bar/*", "event/foo/*", "event/+", "event/+/+", "event/+/*", "bigevent/+/*", "bigevent/foo/data"),
			   Arrays.asList("*"));
	   doTest(selectors, "event/*", Arrays.asList("event/+/data", "event/foo/data", "event/foo/+",
			   "event/bar/*", "event/foo/*", "event/+", "event/+/+", "event/+/*", "bigevent/+/*", "bigevent/foo/data"),
			   Arrays.asList("*", "+"));
	   doTest(selectors, "event/+/*", Arrays.asList("event/+/data", "event/foo/data", "event/foo/+",
			   "event/bar/*", "event/foo/*", "event/+", "event/+/+", "bigevent/+/*", "bigevent/foo/data"),
			   Arrays.asList("*", "+", "event/*"));
	   doTest(selectors, "bigevent/+/*", Arrays.asList("event/+/data", "event/foo/data", "event/foo/+",
			   "event/bar/*", "event/foo/*", "event/+", "event/+/+", "bigevent/foo/data"),
			   Arrays.asList("*", "+", "event/*", "event/+/*"));
	   doTest(selectors, "event/+/+", Arrays.asList("event/+/data", "event/+", "event/foo/data", "event/foo/+",
			   "event/bar/*", "event/foo/*", "bigevent/foo/data"),
			   Arrays.asList("*", "+", "event/*", "event/+/*", "bigevent/+/*"));
	   doTest(selectors, "event/+/data", Arrays.asList("event/+", "event/foo/data", "event/foo/+",
			   "event/bar/*", "event/foo/*", "bigevent/foo/data"),
			   Arrays.asList("*", "+", "event/*", "event/+/*", "event/+/+", "bigevent/+/*"));
	   doTest(selectors, "event/+", Arrays.asList("event/foo/data", "event/foo/+",
			   "event/bar/*", "event/foo/*", "bigevent/foo/data"),
			   Arrays.asList("*", "+", "event/*", "event/+/*", "event/+/+", "bigevent/+/*", "event/+/data"));
	   doTest(selectors, "event/foo/*", Arrays.asList("event/foo/data", "event/foo/+", "event/bar/*", "bigevent/foo/data"),
			   Arrays.asList("*", "+", "event/*", "event/+/*", "event/+/+", "bigevent/+/*", "event/+/data",
					   "event/+"));
	   doTest(selectors, "event/bar/*", Arrays.asList("event/foo/data", "event/foo/+", "bigevent/foo/data"),
			   Arrays.asList("*", "+", "event/*", "event/+/*", "event/+/+", "bigevent/+/*", "event/+/data",
					   "event/+", "event/foo/*"));
	   doTest(selectors, "event/foo/+", Arrays.asList("event/foo/data", "bigevent/foo/data"),
			   Arrays.asList("*", "+", "event/*", "event/+/*", "event/+/+", "bigevent/+/*", "event/+/data",
					   "event/+", "event/foo/*", "event/bar/*"));
	   doTest(selectors, "event/foo/data", Arrays.asList("bigevent/foo/data"),
			   Arrays.asList("*", "+", "event/*", "event/+/*", "event/+/+", "bigevent/+/*", "event/+/data",
					   "event/+", "event/foo/*", "event/bar/*", "event/foo/+"));
	   doTest(selectors, "bigevent/foo/data", Collections.emptyList(),
			   Arrays.asList("*", "+", "event/*", "event/+/*", "event/+/+", "bigevent/+/*", "event/+/data",
					   "event/+", "event/foo/*", "event/bar/*", "event/foo/+", "event/foo/data"));
   }

	private void doTest(Map<String, EventSelector> selectors, String toTest, List<String> lower, List<String> higher) {
		EventSelector es = new EventSelector(toTest, null);
		EventSelector check = selectors.get(toTest);
		assertEquals(0, es.compareTo(check));
		assertEquals(0, es.compareTo(check));
		
		Set<String> tested = new HashSet<>();
		doCheck(es, check, 0, 0);
		tested.add(toTest);
		
		for(String s : lower) {
			doCheck(es, selectors.get(s), 1, -1);
			tested.add(s);
		}
		for(String s : higher) {
			doCheck(es, selectors.get(s), -1, 1);
			tested.add(s);
		}
		
		assertEquals(selectors.keySet(), tested);
	}

	private void doCheck(EventSelector a, EventSelector b, int ab, int ba) {
		int aToB = a.compareTo(b);
		int bToA = b.compareTo(a);
		// Scale
		aToB = aToB == 0 ? 0 : aToB / Math.abs(aToB);
		bToA = bToA == 0 ? 0 : bToA / Math.abs(bToA);
		
		assertEquals(ab, aToB, "Error comparing " + a.getTopicFilter() + " with " + b.getTopicFilter());
		assertEquals(ba, bToA, "Error comparing " + b.getTopicFilter() + " with " + a.getTopicFilter());
	}
}
