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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventSelector implements Comparable<EventSelector> {

	private static final Logger _log = LoggerFactory.getLogger(EventSelector.class);

	private final String topicFilter;
	
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
		this.topicFilter = topic;
		if(_log.isDebugEnabled()) {
			_log.debug("Generating selector for topic {} with filter {}", topic, filter);
		}
		
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

	public boolean matches(String topic, Map<String, Object> event) {
		// Must match the topic, and the filter if set
		return topicMatcher.test(topic) && (filter == null || filter.matches(event));
	}

	public boolean matchesTopic(String topic) {
		return topicMatcher.test(topic);
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
			_log.debug("Topic {} does not match selector {} with initial {} and addtionals {}", 
					topic, topicFilter, initial, additionalSegments);
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
	
	/**
	 * Get the topic filter
	 * @return
	 */
	public String getTopicFilter() {
		return topicFilter;
	}
	
	public boolean isWildcard() {
		return isMultiLevelWildcard || !additionalSegments.isEmpty();
	}

	@Override
	/**
	 * A lower value indicates a more specific match (according to the algorithm
	 * in the specification)
	 */
	public int compareTo(EventSelector o) {
		int result;
		if(isWildcard()) {
			// Either both wildcards, or the non wildcard is more specific (lower)
			result = o.isWildcard() ? compareWildcards(o) : 1;
		} else {
			// If o is a wildcard this is more specific (lower), 
			// otherwise lexical comparison of filter strings
			result = o.isWildcard() ? -1 : initial.compareTo(o.initial);
		}
		
		return result;
	}

	private int compareWildcards(EventSelector o) {
		// The longer initial number of tokens is more specific
		int compare = tokenCount(o.initial) - tokenCount(initial);
		
		// This loop only executes if the initial tokens were the same length
		for(int i = 0; compare == 0 && i < additionalSegments.size(); i++) {
			if(o.additionalSegments.size() > i) {
				// Both have a next (i.e. post '+') segment
				// the bigger number of tokens is more specific (lower)
				compare = tokenCount(o.additionalSegments.get(i)) - tokenCount(additionalSegments.get(i));
			} else {
				// other is out of segments before this is, so this is more specific
				compare = -1;
			}
		}
		
		if(compare == 0) {
			if(o.additionalSegments.size() > additionalSegments.size()) {
				// o has more segments overall so it is more specific 
				compare = 1;
			} else if(isMultiLevelWildcard) {
				if(!o.isMultiLevelWildcard) {
					// Multi-level wildcards are the least specific
					compare = 1;
				} else {
					// Both multi-level with the same number of segments
					// fall back to lexical
					compare = initial.compareTo(o.initial);
				}
			} else if(o.isMultiLevelWildcard) {
				// Multi Level is least specific
				compare = -1;
			} else {
				// Neither multilevel with the same number of segments
				// use the first segment which compares in a non zero way
				for(int i = 0; compare == 0 && i < additionalSegments.size(); i++) {
					compare = additionalSegments.get(i).compareTo(o.additionalSegments.get(i));
				}
			}
		}
		
		return compare;
	}
	
	private int tokenCount(String s) {
		int count;
		if("/".equals(s)) {
			count = 0;
		} else {
			count = 1;
			int idx = 0;
			while((idx = s.indexOf('/', idx + 1)) > 0) {
				count++;
			}
		}
		return count;
	}
}
