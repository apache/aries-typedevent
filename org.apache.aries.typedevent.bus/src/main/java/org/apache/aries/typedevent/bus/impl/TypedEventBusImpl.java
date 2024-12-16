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

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.osgi.namespace.implementation.ImplementationNamespace.IMPLEMENTATION_NAMESPACE;
import static org.osgi.namespace.service.ServiceNamespace.SERVICE_NAMESPACE;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_FILTER;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_IMPLEMENTATION;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_SPECIFICATION_VERSION;
import static org.osgi.util.converter.Converters.standardConverter;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.apache.aries.typedevent.bus.spi.AriesTypedEvents;
import org.apache.aries.typedevent.bus.spi.CustomEventConverter;
import org.osgi.annotation.bundle.Capability;
import org.osgi.framework.Bundle;
import org.osgi.framework.Constants;
import org.osgi.framework.Filter;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.typedevent.TypedEventBus;
import org.osgi.service.typedevent.TypedEventConstants;
import org.osgi.service.typedevent.TypedEventHandler;
import org.osgi.service.typedevent.UnhandledEventHandler;
import org.osgi.service.typedevent.UntypedEventHandler;
import org.osgi.util.converter.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Capability(namespace=SERVICE_NAMESPACE, attribute="objectClass:List<String>=org.osgi.service.typedevent.TypedEventBus", uses=TypedEventBus.class)
@Capability(namespace=IMPLEMENTATION_NAMESPACE, name=TYPED_EVENT_IMPLEMENTATION, version=TYPED_EVENT_SPECIFICATION_VERSION, uses=TypedEventBus.class)
public class TypedEventBusImpl implements TypedEventBus, AriesTypedEvents {

	private static final Logger _log = LoggerFactory.getLogger(TypedEventBusImpl.class);
	
    private static final TypeReference<List<String>> LIST_OF_STRINGS = new TypeReference<List<String>>() {
    };

    private final Object lock = new Object();

    private final TypedEventMonitorImpl monitorImpl;

    /**
     * Map access and mutation must be synchronized on {@link #lock}. Values from
     * the map should be copied as the contents are not thread safe.
     */
    private final Map<String, Map<TypedEventHandler<?>, EventSelector>> topicsToTypedHandlers = new HashMap<>();
  
    /**
     * Map access and mutation must be synchronized on {@link #lock}. Values from
     * the map should be copied as the contents are not thread safe.
     */
    private final Map<String, Map<TypedEventHandler<?>, EventSelector>> wildcardTopicsToTypedHandlers = new HashMap<>();

    /**
     * Map access and mutation must be synchronized on {@link #lock}. Values from
     * the map should be copied as the contents are not thread safe.
     */
    private final Map<TypedEventHandler<?>, TypeData> typedHandlersToTargetClasses = new HashMap<>();

    /**
     * Map access and mutation must be synchronized on {@link #lock}. Values from
     * the map should be copied as the contents are not thread safe.
     */
    private final Map<String, Map<UntypedEventHandler, EventSelector>> topicsToUntypedHandlers = new HashMap<>();

    /**
     * Map access and mutation must be synchronized on {@link #lock}. Values from
     * the map should be copied as the contents are not thread safe.
     */
    private final Map<String, Map<UntypedEventHandler, EventSelector>> wildcardTopicsToUntypedHandlers = new HashMap<>();

    /**
     * List access and mutation must be synchronized on {@link #lock}.
     */
    private final List<UnhandledEventHandler> unhandledEventHandlers = new ArrayList<>();

    /**
     * Map access and mutation must be synchronized on {@link #lock}. Values from
     * the map should be copied as the contents are not thread safe.
     */
    private final Map<Long, List<String>> knownHandlers = new HashMap<>();

    /**
     * Map access and mutation must be synchronized on {@link #lock}. Values from
     * the map should be copied as the contents are not thread safe.
     */
    private final Map<Long, TypedEventHandler<?>> knownTypedHandlers = new HashMap<>();

    /**
     * Map access and mutation must be synchronized on {@link #lock}. Values from
     * the map should be copied as the contents are not thread safe.
     */
    private final Map<Long, UntypedEventHandler> knownUntypedHandlers = new HashMap<>();

    private final BlockingQueue<EventTask> queue = new LinkedBlockingQueue<>();

    /**
     * 
     * Field access must be synchronized on {@link #threadLock}
     */
    private EventThread thread;

    private final Object threadLock = new Object();
    
    private final boolean allowSingleLevelWildcards;

    /**
     * Access and mutation must be synchronized on {@link #lock}.
     */
	private CustomEventConverter customEventConverter;

    public TypedEventBusImpl(TypedEventMonitorImpl monitorImpl, Map<String, ?> config) {
        this.monitorImpl = monitorImpl;
        this.allowSingleLevelWildcards = Boolean.parseBoolean(String.valueOf(config.get("extended.wildcards.enabled")));
    }

    public void registerGlobalEventConverter(CustomEventConverter converter, boolean force) {
    	synchronized (lock) {
    		if(customEventConverter != null && !force) {
    			throw new IllegalStateException("A custom converter is already set");
    		}
    		customEventConverter = converter;
		}
    	if(_log.isInfoEnabled()) {
    		_log.info("A Custom Event Converter was registered");
    	}
    }

    void addTypedEventHandler(Bundle registeringBundle, TypedEventHandler<?> handler, Map<String, Object> properties) {
        Class<?> clazz = discoverTypeForTypedHandler(registeringBundle, handler, properties);
        
        String defaultTopic = clazz == null ? null : clazz.getName().replace(".", "/");

        doAddEventHandler(topicsToTypedHandlers, wildcardTopicsToTypedHandlers, knownTypedHandlers, handler, defaultTopic, properties);
    }

    private Class<?> discoverTypeForTypedHandler(Bundle registeringBundle, TypedEventHandler<?> handler, Map<String, Object> properties) {
        Type genType = null;
        Object type = properties.get(TypedEventConstants.TYPED_EVENT_TYPE);
        if (type != null) {
            try {
                 genType = registeringBundle.loadClass(String.valueOf(type));
            } catch (ClassNotFoundException e) {
                _log.error("Unable to load the declared event type {} from bundle {}", type, registeringBundle, e);
            }
        } else {
            Class<?> toCheck = handler.getClass();
            outer: while(genType == null) {
                genType = findDirectlyImplemented(toCheck);
                
                if(genType != null) {
                    break outer;
                }
                
                genType = processInterfaceHierarchyForClass(toCheck);

                if(genType != null) {
                    break outer;
                }
                
                toCheck = toCheck.getSuperclass();
            }
        }

        if (genType != null) {
        	TypeData typeData = new TypeData(genType);
            synchronized (lock) {
				typedHandlersToTargetClasses.put(handler, typeData);
            }
            return typeData.getRawType();
        } else {
        	_log.error("Unable to determine the declared event type for service {} from bundle {}", getServiceId(properties), registeringBundle);
        	throw new IllegalArgumentException("Unable to determine handled type for " + handler);
        }
    }

    private Type processInterfaceHierarchyForClass(Class<?> toCheck) {
        Type type = null;
        for (Class<?> iface : toCheck.getInterfaces()) {
            type = findDirectlyImplemented(iface);
            
            if(type != null) {
                break;
            }
            
            type = processInterfaceHierarchyForClass(iface); 

            if(type != null) {
                break;
            }
        }
        return type;
    }

    private Type findDirectlyImplemented(Class<?> toCheck) {
        return Arrays.stream(toCheck.getGenericInterfaces())
            .filter(ParameterizedType.class::isInstance)
            .map(ParameterizedType.class::cast)
            .filter(t -> TypedEventHandler.class.equals(t.getRawType())).map(t -> t.getActualTypeArguments()[0])
            .findFirst().orElse(null);
    }

    void addUntypedEventHandler(UntypedEventHandler handler, Map<String, Object> properties) {
        doAddEventHandler(topicsToUntypedHandlers, wildcardTopicsToUntypedHandlers, knownUntypedHandlers, handler, null, properties);
    }

    private <T> void doAddEventHandler(Map<String, Map<T, EventSelector>> map, Map<String, Map<T, EventSelector>> wildcardMap, 
    		Map<Long, T> idMap, T handler, String defaultTopic, Map<String, Object> properties) {

        Object prop = properties.get(TypedEventConstants.TYPED_EVENT_TOPICS);

        List<String> topicList;
        if (prop == null) {
            if (defaultTopic == null) {
            	_log.error("Unable to determine the registered topics for service {} from service {}", getServiceId(properties));
                return;
            } else {
                topicList = Collections.singletonList(defaultTopic);
            }
        } else {
            topicList = standardConverter().convert(prop).to(LIST_OF_STRINGS);
        }
        
        topicList = topicList.stream()
        		.filter(s -> {
        			String msg = checkTopicSyntax(s, true);
        			if(msg != null) {
        				_log.warn("The topic filter string {} from service {} is not valid: {}", s, getServiceId(properties), msg);
        			}
        			return msg == null;
        		})
        		.collect(toList());
        
        Long serviceId = getServiceId(properties);

        Filter f;
        try {
            f = getFilter(serviceId, properties);
        } catch (IllegalArgumentException e) {
        	_log.error("The event filter from service {} is not valid", getServiceId(properties), e);
            return;
        }

        synchronized (lock) {
            knownHandlers.put(serviceId, topicList);
            idMap.put(serviceId, handler);
        
            for(String s : topicList) {
            	Map<String, Map<T, EventSelector>> mapToUse;
            	String topicToUse;
            	EventSelector selector;
            	if(isWildcard(s)) {
            		mapToUse = wildcardMap;
            		selector = new EventSelector(s, f);
            		topicToUse = selector.getInitial();
            	} else {
            		mapToUse = map;
            		topicToUse = s;
            		selector = new EventSelector(null, f);
            	}
            	Map<T, EventSelector> handlers = mapToUse.computeIfAbsent(topicToUse, x1 -> new HashMap<>());
            	handlers.put(handler, selector);
            }
        }
        if(_log.isDebugEnabled()) {
        	_log.debug("Added service {} as an event handler for topics {}", serviceId, topicList);
        }
    }

    void removeTypedEventHandler(TypedEventHandler<?> handler, Map<String, Object> properties) {

        Long serviceId = getServiceId(properties);

        doRemoveEventHandler(topicsToTypedHandlers, wildcardTopicsToTypedHandlers, knownTypedHandlers, handler, serviceId);

        synchronized (lock) {
            typedHandlersToTargetClasses.remove(handler);
        }
    }

    void removeUntypedEventHandler(UntypedEventHandler handler, Map<String, Object> properties) {

        Long serviceId = getServiceId(properties);

        doRemoveEventHandler(topicsToUntypedHandlers, wildcardTopicsToUntypedHandlers, knownUntypedHandlers, handler, serviceId);
    }

    private Long getServiceId(Map<String, Object> properties) {
        return standardConverter().convert(properties.get(Constants.SERVICE_ID)).to(Long.class);
    }

    private Filter getFilter(Long serviceId, Map<String, Object> properties) throws IllegalArgumentException {
        String key = TYPED_EVENT_FILTER;
        return getFilter(serviceId, key, properties.get(key));
    }

    private Filter getFilter(Long serviceId, String key, Object o) throws IllegalArgumentException {
        if (o == null || "".equals(o)) {
            return null;
        } else {
            try {
                return FrameworkUtil.createFilter(String.valueOf(o));
            } catch (InvalidSyntaxException ise) {
                throw new IllegalArgumentException("The filter associated with property " + key + "for service with id "
                        + serviceId + " is invalid", ise);
            }
        }
    }

    private <T, U> void doRemoveEventHandler(Map<String, Map<T, U>> map, Map<String, Map<T, U>> wildcardMap, Map<Long, T> idMap, 
            T handler, Long serviceId) {
    	List<String> loggable;
        synchronized (lock) {
            List<String> consumed = knownHandlers.remove(serviceId);
            idMap.remove(serviceId);
            if (consumed != null) {
                consumed.forEach(s -> {
                	Map<String, Map<T, U>> handlers;
                	String key;
                	if(isWildcard(s)) {
                		handlers = wildcardMap;
                		key = new EventSelector(s, null).getInitial();
                	} else {
                		handlers = map;
                		key = s;
                	}
                	Map<T,?> subMap = handlers.get(key);
                    if (subMap != null) {
                    	subMap.remove(handler);
                        if (subMap.isEmpty()) {
                            map.remove(key);
                        }
                    }
                });
                loggable = new ArrayList<String>(consumed);
            } else {
            	loggable = Collections.emptyList();
            }
        }
        if(_log.isDebugEnabled()) {
        	_log.debug("Removed service {} as an event handler for topics {}", serviceId, loggable);
        }
    }

    void updatedTypedEventHandler(Bundle registeringBundle, Map<String, Object> properties) {
        Long serviceId = getServiceId(properties);
        TypedEventHandler<?> handler;
        synchronized (lock) {
            handler = knownTypedHandlers.get(serviceId);
        }
        
        Class<?> clazz = discoverTypeForTypedHandler(registeringBundle, handler, properties);
        
        String defaultTopic = clazz == null ? null : clazz.getName().replace(".", "/");
        
        doUpdatedEventHandler(topicsToTypedHandlers, wildcardTopicsToTypedHandlers, knownTypedHandlers, defaultTopic, properties);
    }

    void updatedUntypedEventHandler(Map<String, Object> properties) {
        doUpdatedEventHandler(topicsToUntypedHandlers, wildcardTopicsToUntypedHandlers, knownUntypedHandlers, null, properties);
    }

    private <T> void doUpdatedEventHandler(Map<String, Map<T, EventSelector>> map, Map<String, Map<T, EventSelector>> wildcardMap, Map<Long,T> idToHandler, String defaultTopic,
            Map<String, Object> properties) {
        Long serviceId = getServiceId(properties);

        synchronized (lock) {
            T handler = idToHandler.get(serviceId);
			doRemoveEventHandler(map, wildcardMap, idToHandler, handler, serviceId);
            doAddEventHandler(map, wildcardMap, idToHandler, handler, defaultTopic, properties);
        }
    }

    void addUnhandledEventHandler(UnhandledEventHandler handler, Map<String, Object> properties) {
        synchronized (lock) {
            unhandledEventHandlers.add(handler);
        }
    }

    void removeUnhandledEventHandler(UnhandledEventHandler handler, Map<String, Object> properties) {
        synchronized (lock) {
            unhandledEventHandlers.remove(handler);
        }
    }

    void start() {

        EventThread thread = new EventThread();

        synchronized (threadLock) {
            this.thread = thread;
        }

        thread.start();
    }

    void stop() {
        EventThread thread;

        synchronized (threadLock) {
            thread = this.thread;
            this.thread = null;
        }

        thread.shutdown();

        try {
            thread.join(2000);
        } catch (InterruptedException e) {
            // This is not an error, it just means that we should stop
            // waiting and let the interrupt propagate
            Thread.currentThread().interrupt();
        }
        monitorImpl.destroy();
    }

    @Override
    public void deliver(Object event) {
    	Objects.requireNonNull(event, "The event object must not be null");
        String topicName = event.getClass().getName().replace('.', '/');
        deliver(topicName, event);
    }

    @Override
    public void deliver(String topic, Object event) {
    	checkTopicSyntax(topic);
    	Objects.requireNonNull(event, "The event object must not be null");
        deliver(topic, event, EventConverter::forTypedEvent);
    }

    @Override
    public void deliverUntyped(String topic, Map<String, ?> eventData) {
    	checkTopicSyntax(topic);
    	Objects.requireNonNull(eventData, "The event object must not be null");
        deliver(topic, eventData, EventConverter::forUntypedEvent);
    }

    private <T> void deliver(String topic, T event, 
    		BiFunction<T, CustomEventConverter, EventConverter> eventConversionFactory) {

        List<EventTask> deliveryTasks;

        EventConverter convertibleEventData;
        synchronized (lock) {
			convertibleEventData = eventConversionFactory.apply(event, customEventConverter);
        	
        	List<EventTask> typedDeliveries = toTypedEventTasks(
        			topicsToTypedHandlers.getOrDefault(topic, emptyMap()), topic, convertibleEventData);

            List<EventTask> untypedDeliveries = toUntypedEventTasks(
            		topicsToUntypedHandlers.getOrDefault(topic, emptyMap()), topic, convertibleEventData);

            List<EventTask> wildcardDeliveries = new ArrayList<>();
            String truncatedTopic = topic;
            do {
            	int idx = truncatedTopic.lastIndexOf('/', truncatedTopic.length() - 2);
            	truncatedTopic = idx > 0 ? truncatedTopic.substring(0, idx + 1) : "";
            	wildcardDeliveries.addAll(toTypedEventTasks(
            			wildcardTopicsToTypedHandlers.getOrDefault(truncatedTopic, emptyMap()), 
            			topic, convertibleEventData));
            	wildcardDeliveries.addAll(toUntypedEventTasks(
            			wildcardTopicsToUntypedHandlers.getOrDefault(truncatedTopic, emptyMap()), 
            			topic, convertibleEventData));
            } while (truncatedTopic.length() > 0);

            deliveryTasks = new ArrayList<>(typedDeliveries.size() + untypedDeliveries.size() + wildcardDeliveries.size());

            deliveryTasks.addAll(typedDeliveries);
            deliveryTasks.addAll(untypedDeliveries);
            deliveryTasks.addAll(wildcardDeliveries);
            
            if (deliveryTasks.isEmpty()) {
                if(_log.isDebugEnabled()) {
                	_log.debug("Unhandled Event Handlers are being used for event sent to topic {}", topic);
                }
                deliveryTasks = unhandledEventHandlers.stream()
                        .map(handler -> new UnhandledEventTask(topic, convertibleEventData, handler)).collect(toList());
            }
        }

        queue.add(new MonitorEventTask(topic, convertibleEventData, monitorImpl));

        queue.addAll(deliveryTasks);
    }
    
    private List<EventTask> toTypedEventTasks(Map<TypedEventHandler<?>, EventSelector> map, 
    		String topic, EventConverter convertibleEventData) {
    	List<EventTask> list = new ArrayList<>();
    	for(Entry<TypedEventHandler<?>, EventSelector> e : map.entrySet()) {
    		if(e.getValue().matches(topic, convertibleEventData)) {
    			TypedEventHandler<?> handler = e.getKey();
    			list.add(new TypedEventTask(topic, convertibleEventData, handler,
                typedHandlersToTargetClasses.get(handler)));
    		}
    	}
    	return list;
    }

    private List<EventTask> toUntypedEventTasks(Map<UntypedEventHandler, EventSelector> map, 
    		String topic, EventConverter convertibleEventData) {
    	List<EventTask> list = new ArrayList<>();
    	for(Entry<UntypedEventHandler, EventSelector> e : map.entrySet()) {
    		if(e.getValue().matches(topic, convertibleEventData)) {
    			UntypedEventHandler handler = e.getKey();
    			list.add(new UntypedEventTask(topic, convertibleEventData, handler));
    		}
    	}
    	return list;
    }

    private void checkTopicSyntax(String topic) {
    	String msg = checkTopicSyntax(topic, false);
    	if(msg != null) {
    		throw new IllegalArgumentException(msg);
    	}
    }
    
    private String checkTopicSyntax(String topic, boolean wildcardPermitted) {
    	
    	if(topic == null) {
    		throw new IllegalArgumentException("The topic name is not permitted to be null");
    	}
    	
    	boolean slashPermitted = false;
    	for(int i = 0; i < topic.length(); i++) {
    		int c = topic.codePointAt(i);
    		if(c >= Character.MIN_SUPPLEMENTARY_CODE_POINT) {
    			// handle unicode characters greater than OxFFFF
    			i++;
    		}
    		if('*' == c) {
    			if(!wildcardPermitted) {
    				return "Multi-Level Wildcard topics may not be used for sending events";
    			}
    			if(topic.length() != i + 1) {
    				return "The wildcard * is only permitted at the end of the topic";
    			}
    			if(topic.length() > 1 && topic.codePointAt(i - 1) != '/') {
    				return "The wildcard must be preceded by a / unless it is the only character in the topic string";
    			}
    			continue;
    		}

    		if('+' == c) {
    			if(!allowSingleLevelWildcards) {
    				return "Single Level Wildcard topics are not part of Typed Events 1.0, and must be explicitly enabled using \"extended.wildcards.enabled\"";
    			}
    			if(!wildcardPermitted) {
    				return "Single Level Wildcard topics may not be used for sending events";
    			}
    			if(i > 0 && topic.codePointAt(i - 1) != '/') {
    				return "The single level wildcard must be preceded by a / unless it is the first character in the topic string";
    			}
    			if(topic.length() > i + 1) {
    				if(topic.codePointAt(i + 1) != '/') {
    					return "The single level wildcard must be followed by a / unless it is the last character in the topic string";
    				} else {
    					// We have already checked the next '/' so skip it
    					i++;
    				}
    			}
    			continue;
    		}
    		
    		if('/' == c) {
    			if(slashPermitted && i != (topic.length() - 1)) {
    				slashPermitted = false;
    				continue;
    			}
    		} else if ('-' == c || Character.isJavaIdentifierPart(c)) {
    			slashPermitted = true;
    			continue;
    		}
    		return "Illegal character " + c + " at index " + i + " of topic string: " + topic;
    	}
    	return null;
    }
    
    /**
     * This method assumes that the topic is valid
     * @param topic
     * @return
     */
    private static boolean isWildcard(String topic) {
    	return topic.indexOf('+') >= 0 || topic.indexOf('*') >= 0;
    }
    
    private class EventThread extends Thread {

        private final AtomicBoolean running = new AtomicBoolean(true);

        public EventThread() {
            super("Apache Aries TypedEventBus Delivery Thread");
        }

        public void shutdown() {
            running.set(false);
            interrupt();
        }

        public void run() {

            while (running.get()) {

                EventTask take;
                try {
                    take = queue.take();
                } catch (InterruptedException e) {
                    _log.info("The {} was interrupted while waiting for events", e);
                    continue;
                }

                take.notifyListener();
            }

        }

    }
}
