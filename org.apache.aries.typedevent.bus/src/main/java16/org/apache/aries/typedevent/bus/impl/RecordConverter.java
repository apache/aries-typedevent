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

import static java.util.stream.Collectors.toMap;

import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;

import org.osgi.util.converter.ConversionException;
import org.osgi.util.converter.Converter;
import org.osgi.util.converter.ConverterFunction;

/**
 * This class is responsible for converting Record events to and from their 
 * "flattened" representations. This version runs on Java 16 and above
 */
public class RecordConverter {

    static Object convert(Converter converter, Object o, Type target) {

        if (Record.class.isInstance(o)) {
        	RecordComponent[] sourceComponents = o.getClass().getRecordComponents();
        	
        	if(target instanceof Class<?> clz && Record.class.isAssignableFrom(clz)) {
        		RecordComponent[] targetComponents = clz.getRecordComponents();
        		Object[] args = new Object[targetComponents.length];
        		Class<?>[] argTypes = new Class<?>[targetComponents.length];
        		for(int i = 0; i < targetComponents.length; i++) {
        			RecordComponent targetComponent = targetComponents[i];
					String name = targetComponent.getName();
        			Object arg = null;
        			for(int j = 0; j < sourceComponents.length; j++) {
        				if(sourceComponents[j].getName().equals(name)) {
        					Object sourceArg = getComponentValue(sourceComponents[j], o);
							Type targetArgType = targetComponent.getGenericType();
							arg = converter.convert(sourceArg).to(targetArgType); 
        					break;		
        				}
        			}
        			args[i] = arg;
					argTypes[i] = targetComponent.getType();
        		}
        		return createRecord(clz, args, argTypes);
        	} else {
        		Map<String, Object> converted = Arrays.stream(sourceComponents)
        				.collect(toMap(RecordComponent::getName, rc -> getComponentValue(rc, o)));
        		
        		return converter.convert(converted).to(target);
        	}
        } else if(target instanceof Class<?> clz && Record.class.isAssignableFrom(clz)) {
        	Map<String, Object> intermediate = converter.convert(o).to(EventConverter.MAP_WITH_STRING_KEYS);
        	RecordComponent[] targetComponents = clz.getRecordComponents();
    		Object[] args = new Object[targetComponents.length];
    		Class<?>[] argTypes = new Class<?>[targetComponents.length];
    		for(int i = 0; i < targetComponents.length; i++) {
    			RecordComponent targetComponent = targetComponents[i];
				Object sourceArg = intermediate.get(targetComponent.getName());
				Type targetArgType = targetComponent.getGenericType();
    			args[i] = converter.convert(sourceArg).to(targetArgType); 
				argTypes[i] = targetComponent.getType();
    		}
    		return createRecord(clz, args, argTypes);
        }

        return ConverterFunction.CANNOT_HANDLE;
        
    }

	private static Object createRecord(Class<?> clz, Object[] args, Class<?>[] argTypes) {
		try {
			return clz.getDeclaredConstructor(argTypes).newInstance(args);
		} catch (Exception e) {
			throw new ConversionException("Unable to instantiate record component " + clz.getName(), e);
		}
	}
    
    private static Object getComponentValue(RecordComponent rc, Object o) {
    	try {
    		return rc.getAccessor().invoke(o);
    	} catch (Exception e) {
    		throw new ConversionException("Unable to process record component " + rc.getName() + " from type " + rc.getDeclaringRecord().getName(), e);
    	}
    }
}