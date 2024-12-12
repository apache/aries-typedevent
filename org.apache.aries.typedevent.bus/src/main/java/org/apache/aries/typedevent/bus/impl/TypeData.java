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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 *  The generic type information for the event data
 */
public final class TypeData {
	
	private final Class<?> rawType;
	
	private final Type type;

	public TypeData(Type type) {
		super();
		this.type = type;
		if(type instanceof Class) {
			this.rawType = (Class<?>) type;
		} else if (type instanceof ParameterizedType) {
			this.rawType = (Class<?>) ((ParameterizedType) type).getRawType();
		} else {
			throw new IllegalArgumentException("The type " + type + 
					" is not acceptable. Must be a raw Class or Parameterized Type");
		}
	}

	public Class<?> getRawType() {
		return rawType;
	}

	public Type getType() {
		return type;
	}
}
