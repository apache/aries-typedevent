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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MonitorEventTask extends EventTask {

	private static final Logger _log = LoggerFactory.getLogger(MonitorEventTask.class);
	
    private final String eventType;
    private final EventConverter eventData;
    private final TypedEventMonitorImpl monitorImpl;

    public MonitorEventTask(String eventType, EventConverter eventData, TypedEventMonitorImpl monitorImpl) {
        super();
        this.eventType = eventType;
        this.eventData = eventData;
        this.monitorImpl = monitorImpl;
    }

    @Override
    protected void unsafeNotify() throws Exception {    	
    	if(_log.isTraceEnabled()) {
    		_log.trace("Distributing event to the event monitor");
    	}
    	// Flattening the event is the expensive part, so only do it if someone will see the result
    	if (!monitorImpl.isMonitored(eventType)) {
    		return;
    	}
    	Map<String, Object> untyped = eventData.tryUntypedEvent();
    	if (untyped == null) {
    		// Already logged when the conversion failed
    		_log.debug("The event for topic {} has no untyped form and is not passed to the event monitor", eventType);
    		return;
    	}
    	monitorImpl.event(eventType, untyped);
    }
}