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
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import org.osgi.framework.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventSelector {

	private static final Logger _log = LoggerFactory.getLogger(EventSelector.class);
	
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
		if(_log.isDebugEnabled()) {
			_log.debug("Generating selector for topic {} with filter {}", topic, filter);
		}
		
		this.filter = filter;
		
		if(topic == null) {
			// No topic matching
			additionalSegments = Collections.emptyList();
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
				additionalSegments = Collections.emptyList();
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
				additionalSegments = Collections.unmodifiableList(segments);
			}
			
			if(additionalSegments.isEmpty()) {
				if(isMultiLevelWildcard) {
					if(_log.isDebugEnabled()) {
						_log.debug("No single level wildcards for topic {}. Prefix matching \"{}\" will be used", topic, initial);
					}
					topicMatcher = s -> s.startsWith(initial);
				} else {
					if(_log.isDebugEnabled()) {
						_log.debug("No single level wildcards for topic {}. Exact matching will be used", topic);
					}
					topicMatcher = initial::equals;
				}
			} else {
				if(_log.isDebugEnabled()) {
					_log.debug("Single level wildcards detected for topic {}. Prefix matching \"{}\" will be used", topic, initial);
				}
				topicMatcher = this::topicMatch;
			}
		}
	}
	
	public boolean matches(String topic, EventConverter event) {
		// Must match the topic, and the filter if set
		return topicMatcher.test(topic) && (filter == null || event.applyFilter(filter));
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
					if(_log.isDebugEnabled()) {
						_log.debug("Topic {} does not match selector with initial {} and addtionals {}", 
								topic, initial, additionalSegments);
					}
					// Doesn't match the segment
					return false;
				}
			}
			
			if(startIdx == topic.length() ||
					(isMultiLevelWildcard && topic.charAt(startIdx - 1) == '/')) {
				if(_log.isDebugEnabled()) {
					_log.debug("Topic {} matches selector with initial {} and addtionals {}", 
							topic, initial, additionalSegments);
				}
				// We consumed the whole topic, or the remaining tokens were
				// accepted by a multi-level wildcard, so this is a match.
				return true;
			}
		}
		if(_log.isDebugEnabled()) {
			_log.debug("Topic {} does not match selector with initial {} and addtionals {}", 
					topic, initial, additionalSegments);
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
