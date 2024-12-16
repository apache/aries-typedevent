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

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.ListIterator;
import java.util.stream.Stream;

import org.osgi.service.typedevent.monitor.MonitorEvent;

public abstract class HistoryReplayTask extends EventTask {

	private final TypedEventMonitorImpl monitorImpl;
	private final List<EventSelector> selectors;
	private final Integer history;

	public <T> HistoryReplayTask(TypedEventMonitorImpl monitorImpl, List<EventSelector> selectors,
			Integer history) {
				this.monitorImpl = monitorImpl;
				this.selectors = selectors;
				this.history = history;
	}

	@Override
	protected void unsafeNotify() {
		// In most recent first order
		List<MonitorEvent> events = monitorImpl.copyOfHistory(this::filterHistory);
		
		ListIterator<MonitorEvent> li = events.listIterator(events.size());
		
		while(li.hasPrevious()) {
			notifyListener(li.previous());
		}
	}
	
	private List<MonitorEvent> filterHistory(Stream<MonitorEvent> s) {
		return s.filter(this::selected).limit(history).collect(toList());
	}
	
	private boolean selected(MonitorEvent me) {
		return selectors.stream().anyMatch(es -> es.matches(me.topic, me.eventData));
	}
	
	protected abstract void notifyListener(MonitorEvent me);

}
