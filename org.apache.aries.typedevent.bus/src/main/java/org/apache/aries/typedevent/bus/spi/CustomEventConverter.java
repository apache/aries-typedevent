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
package org.apache.aries.typedevent.bus.spi;

import java.util.Map;

import org.osgi.annotation.versioning.ConsumerType;

/**
 * A Custom Event Converter can be registered to provide customised
 * conversion of event data.
 */
@ConsumerType
public interface CustomEventConverter {
	/**
	 * Convert the supplied event to the target type
	 * @param event - the event to convert
	 * @param typeData - the target type information
	 * @return A converted event, or <code>null</code> if the event cannot be converted
	 */
	public <T> T toTypedEvent(Object event, TypeData typeData);
	
	/**
	 * Convert the supplied event to an untyped event (nested maps of scalar types)
	 * @param event - the event to convert
	 * @return A converted event, or <code>null</code> if the event cannot be converted
	 */
	public Map<String, Object> toUntypedEvent(Object event);
}
