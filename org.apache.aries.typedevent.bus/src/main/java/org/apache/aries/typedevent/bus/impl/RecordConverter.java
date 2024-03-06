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

import java.lang.reflect.Type;

import org.osgi.util.converter.Converter;
import org.osgi.util.converter.ConverterFunction;

/**
 * This class is responsible for converting Record events to and from their 
 * "flattened" representations. As Java 8 doesn't support Records this is
 * not handled
 */
public class RecordConverter {

    static Object convert(Converter converter, Object o, Type target) {
    	return ConverterFunction.CANNOT_HANDLE;
    }

}