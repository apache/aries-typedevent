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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
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

}
