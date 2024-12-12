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

import org.osgi.annotation.versioning.ProviderType;
import org.osgi.service.typedevent.TypedEventBus;

@ProviderType
public interface AriesTypedEvents extends TypedEventBus {
	
	/**
	 * Register a global event converter for this event bus. Equivalent to
	 * {@link #registerGlobalEventConverter(CustomEventConverter, boolean)} passing
	 * <code>false</code> as the <em>force</em> flag.
	 * @param converter the converter to register
	 * @throws IllegalStateException if a converter has already been registered
	 */
	default public void registerGlobalEventConverter(CustomEventConverter converter) throws IllegalStateException {
		registerGlobalEventConverter(converter, false);
	}

	/**
	 * Register a global event converter for this event bus.
	 * @param converter - the converter to register
	 * @param force - if true then any existing converter will be replaced
	 * @throws IllegalStateException if <em>force</em> is <code>false</code> 
	 * and a converter has already been registered
	 */
	public void registerGlobalEventConverter(CustomEventConverter converter, boolean force) throws IllegalStateException;

}
