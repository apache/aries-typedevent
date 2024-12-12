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

import java.util.List;

import org.apache.aries.typedevent.bus.spi.CustomEventConverter;
import org.osgi.service.typedevent.TypedEventHandler;
import org.osgi.service.typedevent.monitor.MonitorEvent;

public class TypedHistoryReplayTask extends HistoryReplayTask {

	private final CustomEventConverter customEventConverter;
	private final TypedEventHandler<Object> handler;
	private final TypeData eventType;

	@SuppressWarnings("unchecked")
	public TypedHistoryReplayTask(TypedEventMonitorImpl monitorImpl, 
			CustomEventConverter customEventConverter, TypedEventHandler<?> handler, 
			TypeData eventType, List<EventSelector> selectors, Integer history) {
		super(monitorImpl, selectors, history);
		this.customEventConverter = customEventConverter;
		this.handler = (TypedEventHandler<Object>) handler;
		this.eventType = eventType;
	}

	@Override
	protected void notifyListener(MonitorEvent me) {
		handler.notify(me.topic, (Object) EventConverter.forUntypedEvent(me.eventData, customEventConverter).toTypedEvent(eventType));
	}

}
