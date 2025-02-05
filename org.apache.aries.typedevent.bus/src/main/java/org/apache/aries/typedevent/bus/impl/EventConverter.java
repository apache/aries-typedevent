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

import static java.lang.ThreadLocal.withInitial;
import static java.util.Collections.newSetFromMap;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.aries.typedevent.bus.spi.CustomEventConverter;
import org.apache.aries.typedevent.bus.spi.TypeData;
import org.osgi.framework.Filter;
import org.osgi.util.converter.Converter;
import org.osgi.util.converter.ConverterFunction;
import org.osgi.util.converter.Converters;
import org.osgi.util.converter.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for converting events to and from their "flattened"
 * representations. A "flattened" event uses Maps rather than DTOs and Strings
 * rather than enums. This makes a flattened event readable by anyone, even if
 * they don't have access to the necessary API types
 */
public class EventConverter {

	private static final Logger _log = LoggerFactory.getLogger(EventConverter.class);
	
    private static final TypeReference<List<Object>> LIST_OF_OBJECTS = new TypeReference<List<Object>>() {
    };
    private static final TypeReference<Set<Object>> SET_OF_OBJECTS = new TypeReference<Set<Object>>() {
    };
    static final TypeReference<Map<String, Object>> MAP_WITH_STRING_KEYS = new TypeReference<Map<String, Object>>() {
    };
    private static final TypeReference<Map<Object, Object>> MAP_OF_OBJECT_TO_OBJECT = new TypeReference<Map<Object, Object>>() {
    };

    private static final Converter eventConverter;
    private static final Set<Class<?>> safeClasses;
    private static final Set<Class<?>> specialClasses;
    private static final ThreadLocal<Set<Object>> errorsBeingHandled = withInitial(
            () -> newSetFromMap(new IdentityHashMap<>()));

    static {
        safeClasses = new HashSet<>();
        safeClasses.add(String.class);
        safeClasses.add(Boolean.class);
        safeClasses.add(Byte.class);
        safeClasses.add(Short.class);
        safeClasses.add(Character.class);
        safeClasses.add(Integer.class);
        safeClasses.add(Long.class);
        safeClasses.add(Float.class);
        safeClasses.add(Double.class);

        specialClasses = new HashSet<Class<?>>();
        specialClasses.add(Date.class);
        specialClasses.add(Calendar.class);
        specialClasses.add(Duration.class);
        specialClasses.add(Instant.class);
        specialClasses.add(LocalDate.class);
        specialClasses.add(LocalDateTime.class);
        specialClasses.add(LocalTime.class);
        specialClasses.add(MonthDay.class);
        specialClasses.add(OffsetTime.class);
        specialClasses.add(OffsetDateTime.class);
        specialClasses.add(Year.class);
        specialClasses.add(YearMonth.class);
        specialClasses.add(ZonedDateTime.class);
        specialClasses.add(UUID.class);

        eventConverter = Converters.standardConverter().newConverterBuilder()
        		.rule(EventConverter::convertRecord)
        		.rule(EventConverter::convert)
                .errorHandler(EventConverter::attemptRecovery).build();
    }

    static Object convertRecord(Object o, Type target) {
    	return RecordConverter.convert(eventConverter, o, target);
    }
    
    /**
     * Conversion for nested Map values
     * @param o - the value to convert
     * @param target - the target - should be Object.class
     * @return
     */
    static Object convert(Object o, Type target) {

        if (target != Object.class || o == null) {
            return ConverterFunction.CANNOT_HANDLE;
        }

        Class<? extends Object> sourceClass = o.getClass();

        // "Safe" classes use an identity transform
        if (safeClasses.contains(sourceClass)) {
        	if(_log.isTraceEnabled()) {
        		_log.trace("The object {} does not require conversion.", o);
        	}
            return o;
        }

        // "Special" types and Enums map to strings
        if (specialClasses.contains(sourceClass) || sourceClass.isEnum()) {
        	if(_log.isTraceEnabled()) {
        		_log.trace("The object {} will be mapped as a String.", o);
        	}
            return eventConverter.convert(o).sourceAs(Object.class).to(String.class);
        }

        // Collections get remapped using the same converter to
        // the relevant collection type containing objects, this
        // ensures we pick up any embedded lists of DTOs or enums
        if (o instanceof Collection) {
        	if(_log.isTraceEnabled()) {
        		_log.trace("The collection {} will be processed to map the contained objects.", o);
        	}
            if (o instanceof Set) {
                return eventConverter.convert(o).to(SET_OF_OBJECTS);
            } else {
                return eventConverter.convert(o).to(LIST_OF_OBJECTS);
            }
        }

        // As with collections we remap nested maps to clean up any
        // undesirable types in the keys or values
        if (o instanceof Map) {
        	if(_log.isTraceEnabled()) {
        		_log.trace("The map {} will be processed to remap the contained objects.", o);
        	}
            return eventConverter.convert(o).to(MAP_OF_OBJECT_TO_OBJECT);
        }

        if (sourceClass.isArray()) {
        	if(_log.isTraceEnabled()) {
        		_log.trace("The array {} will be processed to remap the contained objects.", o);
        	}
            int depth = 1;
            Class<?> arrayComponentType = sourceClass.getComponentType();
            Class<?> actualComponentType = sourceClass.getComponentType();
            while (actualComponentType.isArray()) {
                depth++;
                actualComponentType = actualComponentType.getComponentType();
            }
            if (safeClasses.contains(actualComponentType) || actualComponentType.isPrimitive()) {
            	if(_log.isTraceEnabled()) {
            		_log.trace("The array {} does not need conversion.", o);
            	}
                return o;
            } else if (actualComponentType.isEnum()) {
            	if(_log.isTraceEnabled()) {
            		_log.trace("The array {} will use String values.", o);
            	}
                // This becomes an n dimensional String array
                Class<?> stringArrayType = Array.newInstance(String.class, new int[depth]).getClass();
                return eventConverter.convert(o).to(stringArrayType);
            } else {
            	if(_log.isTraceEnabled()) {
            		_log.trace("The array {} is complex and will be treated as a list.", o);
            	}
                // This is an array of something complicated, recursively turn it into a
                // list of something, then make it into an array of the right type
                List<Object> oList = eventConverter.convert(o).to(LIST_OF_OBJECTS);
                return oList.toArray((Object[]) Array.newInstance(arrayComponentType, 0));
            }
        }

        if(_log.isTraceEnabled()) {
    		_log.trace("The object {} will be treated as a DTO.", o);
    	}
        // If we get here then treat the type as a DTO
        return eventConverter.convert(o).sourceAsDTO().to(MAP_WITH_STRING_KEYS);
    }

    static Object attemptRecovery(Object o, Type target) {
        if (o instanceof Map) {
            Set<Object> errors = errorsBeingHandled.get();

            if (errors.contains(o)) {
            	if(_log.isWarnEnabled()) {
            		_log.warn("The map {} repeatedly failed conversion to type {}. This event cannot be converted", o, target);
            	}
                return ConverterFunction.CANNOT_HANDLE;
            }

            try {
            	if(_log.isWarnEnabled()) {
            		_log.warn("The map {} failed conversion to type {}. Convertsion will be reattempted treating the target as a DTO", o, target);
            	}
                errors.add(o);
                return eventConverter.convert(o).targetAsDTO().to(target);
            } finally {
                errors.remove(o);
            }
        }
        return ConverterFunction.CANNOT_HANDLE;
    }

    private final Object originalEvent;
    private final CustomEventConverter custom;

    private Map<String, Object> untypedEventDataForFiltering;

    private EventConverter(Object event, CustomEventConverter custom) {
        this.originalEvent = event;
		this.custom = custom;
    }

    private EventConverter(Map<String, Object> untypedEvent, CustomEventConverter custom) {
        this.originalEvent = untypedEvent;
        this.custom = custom;
        this.untypedEventDataForFiltering = untypedEvent;
    }

    public static EventConverter forTypedEvent(Object event, CustomEventConverter custom) {
        return new EventConverter(event, custom);
    }

    public static EventConverter forUntypedEvent(Map<String, ?> event, CustomEventConverter custom) {
        return new EventConverter(event, custom);
    }

    public boolean applyFilter(Filter f) {
        Map<String, ?> toTest;
        if (untypedEventDataForFiltering == null) {
            toTest = toUntypedEvent();
        } else {
            toTest = untypedEventDataForFiltering;
        }

        return f.matches(toTest);
    }

    public Map<String, Object> toUntypedEvent() {
        if (untypedEventDataForFiltering == null) {
        	if(custom != null) {
        		if(_log.isDebugEnabled()) {
        			_log.debug("Using custom converter to convert {} to untyped data", originalEvent);
        		}
        		untypedEventDataForFiltering = custom.toUntypedEvent(originalEvent);
        		if(untypedEventDataForFiltering == null) {
        			if(_log.isDebugEnabled()) {
        				_log.debug("Custom event converter could not convert event {}. Falling back to built-in conversion",
        						originalEvent);
        			}
        			untypedEventDataForFiltering = eventConverter.convert(originalEvent).sourceAsDTO().to(MAP_WITH_STRING_KEYS);
        		} else {
        			return untypedEventDataForFiltering;
        		}
        	} else {
        		if(_log.isDebugEnabled()) {
        			_log.debug("Converting {} to untyped data", originalEvent);
        		}
        		untypedEventDataForFiltering = eventConverter.convert(originalEvent).sourceAsDTO().to(MAP_WITH_STRING_KEYS);
        	}
        	
        }
        return untypedEventDataForFiltering;
    }

    @SuppressWarnings("unchecked")
	public <T> T toTypedEvent(TypeData typeData) {
		Class<?> rawType = typeData.getRawType();
		Type genericTarget = typeData.getType();
		if (typeData.isForceConversion()) {
			if (_log.isDebugEnabled()) {
				_log.debug("Conversion is forced by property {}", TypeData.ARIES_EVENT_FORCE_CONVERSION);
			}
		} else if (rawType.isInstance(originalEvent) && rawType == genericTarget) {
			if (_log.isDebugEnabled()) {
				_log.debug("No need to convert {} to {} as it is already an instance", originalEvent, rawType);
			}
			return (T) originalEvent;
		}
		if (custom != null) {
			if (_log.isDebugEnabled()) {
				_log.debug("Using custom converter to convert {} to {}", originalEvent, genericTarget);
			}
			Object result = custom.toTypedEvent(originalEvent, typeData);
			if (result != null) {
				return (T) result;
			}
			if (_log.isDebugEnabled()) {
				_log.debug(
						"Custom event converter could not convert event {} to {}. Falling back to built-in conversion",
						originalEvent, genericTarget);
			}
			return eventConverter.convert(originalEvent).targetAsDTO().to(genericTarget);
		} else {
			if (_log.isDebugEnabled()) {
				_log.debug("Converting {} to {}", originalEvent, genericTarget);
			}
			return eventConverter.convert(originalEvent).targetAsDTO().to(genericTarget);
		}
	}

}