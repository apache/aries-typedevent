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
import java.lang.reflect.InvocationTargetException;
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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.aries.typedevent.bus.spi.CustomEventConverter;
import org.apache.aries.typedevent.bus.spi.TypeData;
import org.osgi.framework.Filter;
import org.osgi.util.converter.ConversionException;
import org.osgi.util.converter.Converter;
import org.osgi.util.converter.ConverterFunction;
import org.osgi.util.converter.Converters;
import org.osgi.util.converter.TypeReference;
import org.osgi.util.promise.Deferred;
import org.osgi.util.promise.Promise;
import org.osgi.util.promise.Promises;
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
	
    static final TypeReference<Map<String, Object>> MAP_WITH_STRING_KEYS = new TypeReference<Map<String, Object>>() {
    };

    // Converts a DTO to a Map of its raw field values, without flattening the values
    private static final Converter shallowConverter = Converters.standardConverter();
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
        		_log.trace("The collection of type {} will be processed to map the contained objects.", sourceClass);
        	}
            return flatten(o, o instanceof Set ? new LinkedHashSet<>() : new ArrayList<>(),
            		EventConverter::fillCollection);
        }

        // As with collections we remap nested maps to clean up any
        // undesirable types in the keys or values
        if (o instanceof Map) {
        	if(_log.isTraceEnabled()) {
        		_log.trace("The map of type {} will be processed to remap the contained objects.", sourceClass);
        	}
            return flatten(o, new LinkedHashMap<>(), EventConverter::fillMap);
        }

        if (sourceClass.isArray()) {
        	if(_log.isTraceEnabled()) {
        		_log.trace("The array of type {} will be processed to remap the contained objects.", sourceClass);
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
            		_log.trace("The array of type {} does not need conversion.", sourceClass);
            	}
                return o;
            } else if (actualComponentType.isEnum()) {
            	if(_log.isTraceEnabled()) {
            		_log.trace("The array of type {} will use String values.", sourceClass);
            	}
                // This becomes an n dimensional String array
                Class<?> stringArrayType = Array.newInstance(String.class, new int[depth]).getClass();
                return eventConverter.convert(o).to(stringArrayType);
            } else {
            	if(_log.isTraceEnabled()) {
            		_log.trace("The array of type {} is complex and its elements will be remapped.", sourceClass);
            	}
                // This is an array of something complicated, recursively remap the
                // elements into an array of the right type
                return flatten(o, Array.newInstance(arrayComponentType, Array.getLength(o)),
                		EventConverter::fillArray);
            }
        }

        if(_log.isTraceEnabled()) {
    		_log.trace("The object of type {} will be treated as a DTO.", sourceClass);
    	}
        // If we get here then treat the type as a DTO
        return flatten(o, new LinkedHashMap<String, Object>(), EventConverter::fillDto);
    }

    /**
     * The state of one flattening operation on the current thread
     */
    private static final class FlatteningContext {
        // Maps each source object seen so far to the result built for it
        final Map<Object, Visit> visits = new IdentityHashMap<>();
        // Nested conversions use the same context as the outermost one
        int depth;
        // Set if an object was reached again while it was still being flattened
        boolean cyclic;
    }

    private static final class Visit {
        final Object result;
        boolean inProgress = true;

        Visit(Object result) {
            this.result = result;
        }
    }

    @FunctionalInterface
    private interface Filler {
        void fill(Object source, Object result);
    }

    private static final ThreadLocal<FlatteningContext> flattening = new ThreadLocal<>();

    private static FlatteningContext enterFlattening() {
        FlatteningContext context = flattening.get();
        if (context == null) {
            context = new FlatteningContext();
            flattening.set(context);
        }
        context.depth++;
        return context;
    }

    private static void exitFlattening(FlatteningContext context) {
        if (--context.depth == 0) {
            flattening.remove();
        }
    }

    /**
     * Flatten a source object into the (empty) result container. The result is
     * registered before it is filled, so that if the source refers back to itself
     * the same result is used again, and the flattened form has the same shape.
     */
    private static Object flatten(Object source, Object result, Filler filler) {
        FlatteningContext context = enterFlattening();
        try {
            Visit visit = context.visits.get(source);
            if (visit != null) {
                if (visit.inProgress) {
                    context.cyclic = true;
                }
                return visit.result;
            }
            visit = new Visit(result);
            context.visits.put(source, visit);
            filler.fill(source, result);
            visit.inProgress = false;
            return result;
        } finally {
            exitFlattening(context);
        }
    }

    private static Object flattenValue(Object value) {
        return value == null ? null : eventConverter.convert(value).to(Object.class);
    }

    @SuppressWarnings("unchecked")
    private static void fillCollection(Object source, Object result) {
        Collection<Object> target = (Collection<Object>) result;
        for (Object element : (Collection<?>) source) {
            target.add(flattenValue(element));
        }
    }

    @SuppressWarnings("unchecked")
    private static void fillMap(Object source, Object result) {
        Map<Object, Object> target = (Map<Object, Object>) result;
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) source).entrySet()) {
            target.put(flattenValue(entry.getKey()), flattenValue(entry.getValue()));
        }
    }

    private static void fillArray(Object source, Object result) {
        Object[] target = (Object[]) result;
        Object[] elements = (Object[]) source;
        for (int i = 0; i < elements.length; i++) {
            target[i] = flattenValue(elements[i]);
        }
    }

    @SuppressWarnings("unchecked")
    private static void fillDto(Object source, Object result) {
        Map<String, Object> target = (Map<String, Object>) result;
        Map<String, Object> fields = shallowConverter.convert(source).sourceAsDTO().to(MAP_WITH_STRING_KEYS);
        for (Map.Entry<String, Object> field : fields.entrySet()) {
            target.put(field.getKey(), flattenValue(field.getValue()));
        }
    }

    static Object attemptRecovery(Object o, Type target) {
        if (o instanceof Map) {
            Set<Object> errors = errorsBeingHandled.get();

            if (errors.contains(o)) {
            	if(_log.isWarnEnabled()) {
            		// The map may be cyclic, so don't use its toString
            		_log.warn("A map of type {} repeatedly failed conversion to type {}. This event cannot be converted", o.getClass(), target);
            	}
                return ConverterFunction.CANNOT_HANDLE;
            }

            try {
            	if(_log.isWarnEnabled()) {
            		_log.warn("A map of type {} failed conversion to type {}. Conversion will be reattempted treating the target as a DTO", o.getClass(), target);
            	}
                errors.add(o);
                return eventConverter.convert(o).targetAsDTO().to(target);
            } finally {
                errors.remove(o);
            }
        }
        return ConverterFunction.CANNOT_HANDLE;
    }

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<EventConverter, Promise> FLATTENED = AtomicReferenceFieldUpdater
            .newUpdater(EventConverter.class, Promise.class, "flattened");

    /**
     * The flattened form of an event, and whether it contains a cycle
     */
    private static final class Flattened {
        final Map<String, Object> data;
        final boolean cyclic;

        Flattened(Map<String, Object> data, boolean cyclic) {
            this.data = data;
            this.cyclic = cyclic;
        }
    }

    private final Object originalEvent;
    private final CustomEventConverter custom;

    // Resolved by the first thread that needs the flattened event, so that an event
    // which cannot be flattened fails once rather than once per handler. The field
    // has to be volatile for the updater.
    private volatile Promise<Flattened> flattened;

    // Only used for typed conversions, which are all made by the delivery thread. A
    // stale value only costs another failed attempt.
    private boolean typedConversionOverflowed;

    private EventConverter(Object event, CustomEventConverter custom) {
        this.originalEvent = event;
		this.custom = custom;
    }

    private EventConverter(Map<String, Object> untypedEvent, CustomEventConverter custom) {
        this.originalEvent = untypedEvent;
        this.custom = custom;
        this.flattened = Promises.resolved(new Flattened(untypedEvent, false));
    }

    public static EventConverter forTypedEvent(Object event, CustomEventConverter custom) {
        return new EventConverter(event, custom);
    }

    public static EventConverter forUntypedEvent(Map<String, ?> event, CustomEventConverter custom) {
        return new EventConverter(event, custom);
    }

    public boolean applyFilter(Filter f) {
        Map<String, ?> toTest = tryUntypedEvent();
        // A filter cannot match an event with no untyped form
        return toTest != null && f.matches(toTest);
    }

    /**
     * @return The untyped form of this event. If the event contains a cycle then
     *         the Maps, Lists and Sets of the result do too.
     * @throws ConversionException if the event cannot be converted. The same
     *         exception is thrown each time, as the conversion is not repeated.
     */
    public Map<String, Object> toUntypedEvent() {
        Promise<Flattened> promise = flattened;
        if (promise == null) {
            promise = flatten();
        }
        try {
            return promise.getValue().data;
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            throw cause instanceof ConversionException ? (ConversionException) cause
                    : new ConversionException("Unable to convert an event of type " + originalEvent.getClass().getName(), cause);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ConversionException("Interrupted while converting an event of type " + originalEvent.getClass().getName(), ie);
        }
    }

    /**
     * @return The untyped form of this event, or null if it cannot be converted.
     *         The failure is logged by the first attempt, so callers need not.
     */
    Map<String, Object> tryUntypedEvent() {
        try {
            return toUntypedEvent();
        } catch (ConversionException ce) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private Promise<Flattened> flatten() {
        Deferred<Flattened> deferred = new Deferred<>();
        if (!FLATTENED.compareAndSet(this, null, deferred.getPromise())) {
            // Another thread is converting the event, or has finished
            return (Promise<Flattened>) FLATTENED.get(this);
        }

        try {
            deferred.resolve(flattenOriginalEvent());
        } catch (Throwable t) {
            if (t instanceof Error && !(t instanceof StackOverflowError)) {
                // Don't leave other threads waiting for a result that will never arrive
                deferred.fail(t);
                throw (Error) t;
            }
            Class<?> eventType = originalEvent.getClass();
            // The event may be cyclic, so don't use its toString
            _log.warn("The event of type {} cannot be converted to its untyped form. It will not be delivered to untyped handlers, "
                    + "or be matched by event filters or the event monitor.", eventType.getName(), t);
            deferred.fail(new ConversionException("Unable to convert an event of type " + eventType.getName() + " to its untyped form", t));
        }
        return deferred.getPromise();
    }

    private Flattened flattenOriginalEvent() {
        if (custom != null) {
            if(_log.isDebugEnabled()) {
                _log.debug("Using custom converter to convert an event of type {} to untyped data", originalEvent.getClass());
            }
            Map<String, Object> untyped = custom.toUntypedEvent(originalEvent);
            if (untyped != null) {
                return new Flattened(untyped, false);
            }
            if(_log.isDebugEnabled()) {
                _log.debug("Custom event converter could not convert an event of type {}. Falling back to built-in conversion",
                        originalEvent.getClass());
            }
        } else if(_log.isTraceEnabled()) {
            _log.trace("Converting an event of type {} to untyped data", originalEvent.getClass());
        }

        // The context is shared with the nested conversions, and tells us about cycles
        FlatteningContext context = enterFlattening();
        try {
            Object result = eventConverter.convert(originalEvent).to(Object.class);
            if (!(result instanceof Map)) {
                throw new ConversionException("An event of type " + originalEvent.getClass().getName() + " cannot be converted to a Map");
            }
            return new Flattened(withStringKeys((Map<?, ?>) result), context.cyclic);
        } finally {
            exitFlattening(context);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> withStringKeys(Map<?, ?> map) {
        if (map.keySet().stream().allMatch(String.class::isInstance)) {
            return (Map<String, Object>) map;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            result.put(eventConverter.convert(entry.getKey()).to(String.class), entry.getValue());
        }
        return result;
    }

    private boolean isKnownCyclic() {
        Promise<Flattened> promise = flattened;
        if (promise == null || !promise.isDone()) {
            return false;
        }
        try {
            return promise.getValue().cyclic;
        } catch (InvocationTargetException | InterruptedException e) {
            return false;
        }
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
				_log.debug("No need to convert an event of type {} to {} as it is already an instance", originalEvent.getClass(), rawType);
			}
			// This is also what keeps any cycles in the event intact
			return (T) originalEvent;
		}
		if (custom != null) {
			if (_log.isDebugEnabled()) {
				_log.debug("Using custom converter to convert an event of type {} to {}", originalEvent.getClass(), genericTarget);
			}
			Object result = custom.toTypedEvent(originalEvent, typeData);
			if (result != null) {
				return (T) result;
			}
			if (_log.isDebugEnabled()) {
				_log.debug(
						"Custom event converter could not convert an event of type {} to {}. Falling back to built-in conversion",
						originalEvent.getClass(), genericTarget);
			}
		} else if (_log.isDebugEnabled()) {
			_log.debug("Converting an event of type {} to {}", originalEvent.getClass(), genericTarget);
		}
		return convertToTyped(genericTarget);
	}

	private <T> T convertToTyped(Type genericTarget) {
		if (typedConversionOverflowed || isKnownCyclic()) {
			throw cyclicConversion(genericTarget, null);
		}
		try {
			return eventConverter.convert(originalEvent).targetAsDTO().to(genericTarget);
		} catch (StackOverflowError so) {
			// The converter has no handling for cycles when it builds DTOs
			typedConversionOverflowed = true;
			throw cyclicConversion(genericTarget, so);
		}
	}

	private IllegalArgumentException cyclicConversion(Type genericTarget, Throwable cause) {
		return new IllegalArgumentException("An event of type " + originalEvent.getClass().getName()
				+ " contains a cycle and cannot be converted to " + genericTarget.getTypeName(), cause);
	}

}
