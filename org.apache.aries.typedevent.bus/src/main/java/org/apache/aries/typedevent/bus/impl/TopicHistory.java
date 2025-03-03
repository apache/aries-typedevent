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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.osgi.service.typedevent.monitor.MonitorEvent;
import org.osgi.service.typedevent.monitor.RangePolicy;

public class TopicHistory {
    private final int minRequired;
    private final int maxRequired;
    // Oldest first
    private final Deque<MonitorEvent> events;
    
    public TopicHistory(int minRequired, int maxRequired) {
		this.minRequired = minRequired;
		this.maxRequired = maxRequired;
		events = new ArrayDeque<MonitorEvent>(maxRequired);
	}

	public MonitorEvent addEvent(MonitorEvent event) {
    	MonitorEvent toRemove = events.size() == maxRequired ? events.removeFirst() : null;
    	events.offerLast(event);
    	return toRemove;
    }
    
	public boolean clearEvent(MonitorEvent event) {
		if(events.size() > minRequired) {
			MonitorEvent last = events.remove();
			if(last != event) {
				// TODO log a warning?
			}
			return true;
		} else {
			return false;
		}
	}
	
	public boolean policyMatches(RangePolicy policy) {
		return policy.getMinimum() == minRequired && policy.getMaximum() == maxRequired;
	}

	public List<MonitorEvent> copyFrom(TopicHistory oldHistory) {
		List<MonitorEvent> list = new ArrayList<MonitorEvent>(oldHistory.events);

		int toCopy = Math.min(list.size(), maxRequired - events.size());
		int newSize = list.size() - toCopy;
		for(int i = 0 ; i < toCopy; i++) {
			events.offerLast(list.remove(newSize));
		}
		return list;
	}
}