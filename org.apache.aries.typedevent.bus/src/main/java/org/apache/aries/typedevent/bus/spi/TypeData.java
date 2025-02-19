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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;

/**
 * The generic type information for the event data
 */
public final class TypeData {
	/**
	 * Property name to force the use of the converter. Set the value to true to
	 * force the conversion. Default: false.
	 */
	public static final String ARIES_EVENT_FORCE_CONVERSION = "aries.event.force.conversion";

	private final Class<?> rawType;

	private final Type type;

	private final Map<String, Object> properties;

	private final boolean forceConversion;

	public TypeData(Type type, Map<String, Object> properties) {
		super();
		this.type = type;
		this.properties = Collections.unmodifiableMap(properties);
		this.forceConversion = "true".equalsIgnoreCase(properties.getOrDefault(ARIES_EVENT_FORCE_CONVERSION, "false").toString());
		if (type instanceof Class) {
			this.rawType = (Class<?>) type;
		} else if (type instanceof ParameterizedType) {
			this.rawType = (Class<?>) ((ParameterizedType) type).getRawType();
		} else {
			throw new IllegalArgumentException(
					"The type " + type + " is not acceptable. Must be a raw Class or Parameterized Type");
		}
	}

	/**
	 * Gets the Raw type as {@link Class}
	 * 
	 * @return raw type as Class
	 */
	public Class<?> getRawType() {
		return rawType;
	}

	/**
	 * Gets the Raw type as {@link Type}
	 * 
	 * @return the type as java.lang.reflect.Type
	 */
	public Type getType() {
		return type;
	}

	/**
	 * Gets a unmodifiable {@link Map} of properties from the event handler 
	 * 
	 * @return properties of the event handler
	 */
	public Map<String, Object> getHandlerProperties() {
		return properties;
	}

	/**
	 * Flag set via handler service property
	 * {@link TypeData#ARIES_EVENT_FORCE_CONVERSION} to force the conversion of
	 * identical event types.
	 * 
	 * @return <code>true</code> if the conversion is forced, <code>false</code> default
	 */
	public boolean isForceConversion() {
		return forceConversion;
	}

	@Override
	public String toString() {
		return "TypeData [type=" + type + "]";
	}
}
