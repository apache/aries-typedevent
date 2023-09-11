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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.osgi.framework.Filter;

public class EventSelector {

	/** The event filter **/
	private final Filter filter;
	
	/** 
	 * Additional topic segments to check after intitial
	 * Each segment starts with a '/' and is preceded by 
	 * a single level wildcard, e.g.
	 * 
	 * "foo/+/foobar/fizz/+/fizzbuzz/done" =>
	 * ["/foobar/fizz/","/fizzbuzz/done"]
	 **/
	private final List<String> additionalSegments;
	
	/**
	 * True if there is a trailing multi-level wildcard
	 */
	private final boolean isMultiLevelWildcard;
	
	/**
	 * The initial section of topic to match, 
	 * will only ever contain literals
	 * e.g.
	 * 
	 * "*" => ""
	 * "foo/+/foobar" => "foo/"
	 */
	private final String initial;
	
	private final Predicate<String> topicMatcher;
	
	/**
	 * Create an event selector
	 * 
	 * @param topic - if non null then assumed to be valid. If null then topic checking disabled
	 * @param filter
	 */
	public EventSelector(String topic, Filter filter) {
		this.filter = filter;
		
		if(topic == null) {
			// No topic matching
			additionalSegments = List.of();
			isMultiLevelWildcard = false;
			initial = "";
			topicMatcher = s -> true;
		} else {
			// Do topic matching
			if(topic.endsWith("*")) {
				isMultiLevelWildcard = true;
				topic = topic.substring(0, topic.length() - 1);
			} else {
				isMultiLevelWildcard = false;
			}
			
			int singleLevelIdx = topic.indexOf('+');
			if(singleLevelIdx < 0) {
				initial = topic;
				additionalSegments = List.of();
			} else {
				initial = topic.substring(0, singleLevelIdx);
				List<String> segments = new ArrayList<>();
				for(;;) {
					int nextIdx = topic.indexOf('+', singleLevelIdx + 1);
					if(nextIdx < 0) {
						segments.add(topic.substring(singleLevelIdx + 1));
						break;
					} else {
						segments.add(topic.substring(singleLevelIdx + 1, nextIdx));
						singleLevelIdx = nextIdx;
					}
				}
				additionalSegments = List.copyOf(segments);
			}
			
			if(additionalSegments.isEmpty()) {
				if(isMultiLevelWildcard) {
					topicMatcher = s -> s.startsWith(initial);
				} else {
					topicMatcher = initial::equals;
				}
			} else {
				topicMatcher = this::topicMatch;
			}
		}
	}
	
	public boolean matches(String topic, EventConverter event) {
		// Must match the topic, and the filter if set
		return topicMatcher.test(topic) && (filter == null || event.applyFilter(filter));
	}

	public boolean matches(String topic, Map<String, Object> event) {
		// Must match the topic, and the filter if set
		return topicMatcher.test(topic) && (filter == null || filter.matches(event));
	}
	
	private boolean topicMatch(String topic) {
		
		if(topic.startsWith(initial)) {
			int startIdx = initial.length();
			for(String segment : additionalSegments) {
				// First, skip the single level wildcard
				startIdx = topic.indexOf('/', startIdx);
				if(startIdx < 0) {
					startIdx = topic.length();
				}
				if(topic.regionMatches(startIdx, segment, 0, segment.length())) {
					// Check the next segment
					startIdx += segment.length();
				} else {
					// Doesn't match the segment
					return false;
				}
			}
			
			if(startIdx == topic.length()) {
				// We consumed the whole topic so this is a match
				return true;
			} else if(isMultiLevelWildcard && topic.charAt(startIdx - 1) == '/') {
				// We consumed a whole number of tokens and are multi-level
				return true;
			}
		}
		
		return false;
	}

	/**
	 * Get the initial prefix before the first wildcard
	 * @return
	 */
	public String getInitial() {
		return initial;
	}
}
