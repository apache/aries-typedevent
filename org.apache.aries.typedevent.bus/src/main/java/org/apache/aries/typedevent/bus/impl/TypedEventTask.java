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

import org.osgi.service.typedevent.TypedEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TypedEventTask extends EventTask {
	
	private static final Logger _log = LoggerFactory.getLogger(TypedEventTask.class);
	
    private final String topic;
    private final TypeData targetEventClass;
    private final EventConverter eventData;
    private final TypedEventHandler<Object> eventProcessor;

    @SuppressWarnings("unchecked")
    public TypedEventTask(String topic, EventConverter eventData, TypedEventHandler<?> eventProcessor,
            TypeData targetEventClass) {
        super();
        this.topic = topic;
        this.targetEventClass = targetEventClass;
        this.eventData = eventData;
        this.eventProcessor = (TypedEventHandler<Object>) eventProcessor;
    }

    @Override
    public void unsafeNotify() {
    	if(_log.isDebugEnabled()) {
    		_log.debug("Distributing event data {} to the typed handler {}", eventData, eventProcessor);
    	}
        eventProcessor.notify(topic, eventData.toTypedEvent(targetEventClass));
    }
}