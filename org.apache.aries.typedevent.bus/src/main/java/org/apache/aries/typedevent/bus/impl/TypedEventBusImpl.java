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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

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

@Capability(namespace=SERVICE_NAMESPACE, attribute="objectClass:List<String>=org.osgi.service.typedevent.TypedEventBus", uses=TypedEventBus.class)
@Capability(namespace=IMPLEMENTATION_NAMESPACE, name=TYPED_EVENT_IMPLEMENTATION, version=TYPED_EVENT_SPECIFICATION_VERSION, uses=TypedEventBus.class)
public class TypedEventBusImpl implements TypedEventBus {

    private static final TypeReference<List<String>> LIST_OF_STRINGS = new TypeReference<List<String>>() {
    };

    private final Object lock = new Object();

    private final TypedEventMonitorImpl monitorImpl;

    /**
     * Map access and mutation must be synchronized on {@link #lock}. Values from
     * the map should be copied as the contents are not thread safe.
     */
    private final Map<String, Map<TypedEventHandler<?>, Filter>> topicsToTypedHandlers = new HashMap<>();
  
    /**
     * Map access and mutation must be synchronized on {@link #lock}. Values from
     * the map should be copied as the contents are not thread safe.
     */
    private final NavigableMap<String, Map<TypedEventHandler<?>, Filter>> wildcardTopicsToTypedHandlers = new TreeMap<>();

    /**
     * Map access and mutation must be synchronized on {@link #lock}. Values from
     * the map should be copied as the contents are not thread safe.
     */
    private final Map<TypedEventHandler<?>, Class<?>> typedHandlersToTargetClasses = new HashMap<>();

    /**
     * Map access and mutation must be synchronized on {@link #lock}. Values from
     * the map should be copied as the contents are not thread safe.
     */
    private final NavigableMap<String, Map<UntypedEventHandler, Filter>> topicsToUntypedHandlers = new TreeMap<>();

    /**
     * Map access and mutation must be synchronized on {@link #lock}. Values from
     * the map should be copied as the contents are not thread safe.
     */
    private final Map<String, Map<UntypedEventHandler, Filter>> wildcardTopicsToUntypedHandlers = new HashMap<>();

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

    public TypedEventBusImpl(TypedEventMonitorImpl monitorImpl, Map<String, ?> config) {
        this.monitorImpl = monitorImpl;
    }

    void addTypedEventHandler(Bundle registeringBundle, TypedEventHandler<?> handler, Map<String, Object> properties) {
        Class<?> clazz = discoverTypeForTypedHandler(registeringBundle, handler, properties);
        
        String defaultTopic = clazz == null ? null : clazz.getName().replace(".", "/");

        doAddEventHandler(topicsToTypedHandlers, wildcardTopicsToTypedHandlers, knownTypedHandlers, handler, defaultTopic, properties);
    }

    private Class<?> discoverTypeForTypedHandler(Bundle registeringBundle, TypedEventHandler<?> handler, Map<String, Object> properties) {
        Class<?> clazz = null;
        Object type = properties.get(TypedEventConstants.TYPED_EVENT_TYPE);
        if (type != null) {
            try {
                 clazz = registeringBundle.loadClass(String.valueOf(type));
            } catch (ClassNotFoundException e) {
                // TODO Blow up
                e.printStackTrace();
            }
        } else {
            Class<?> toCheck = handler.getClass();
            outer: while(clazz == null) {
                clazz = findDirectlyImplemented(toCheck);
                
                if(clazz != null) {
                    break outer;
                }
                
                clazz = processInterfaceHierarchyForClass(toCheck);

                if(clazz != null) {
                    break outer;
                }
                
                toCheck = toCheck.getSuperclass();
            }
        }

        if (clazz != null) {
            synchronized (lock) {
                typedHandlersToTargetClasses.put(handler, clazz);
            }
        } else {
            // TODO Blow Up
        }
        return clazz;
    }

    private Class<?> processInterfaceHierarchyForClass(Class<?> toCheck) {
        Class<?> clazz = null;
        for (Class<?> iface : toCheck.getInterfaces()) {
            clazz = findDirectlyImplemented(iface);
            
            if(clazz != null) {
                break;
            }
            
            clazz = processInterfaceHierarchyForClass(iface); 

            if(clazz != null) {
                break;
            }
        }
        return clazz;
    }

    private Class<?> findDirectlyImplemented(Class<?> toCheck) {
        return Arrays.stream(toCheck.getGenericInterfaces())
            .filter(ParameterizedType.class::isInstance)
            .map(ParameterizedType.class::cast)
            .filter(t -> TypedEventHandler.class.equals(t.getRawType())).map(t -> t.getActualTypeArguments()[0])
            .findFirst().map(Class.class::cast).orElse(null);
    }

    void addUntypedEventHandler(UntypedEventHandler handler, Map<String, Object> properties) {
        doAddEventHandler(topicsToUntypedHandlers, wildcardTopicsToUntypedHandlers, knownUntypedHandlers, handler, null, properties);
    }

    private <T> void doAddEventHandler(Map<String, Map<T, Filter>> map, Map<String, Map<T, Filter>> wildcardMap, 
    		Map<Long, T> idMap, T handler, String defaultTopic, Map<String, Object> properties) {

        Object prop = properties.get(TypedEventConstants.TYPED_EVENT_TOPICS);

        List<String> topicList;
        if (prop == null) {
            if (defaultTopic == null) {
                // TODO log a broken handler
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
        				// TODO log this
        			}
        			return msg == null;
        		})
        		.collect(toList());
        
        Long serviceId = getServiceId(properties);

        Filter f;
        try {
            f = getFilter(serviceId, properties);
        } catch (IllegalArgumentException e) {
            // TODO Log a broken handler
            e.printStackTrace();
            return;
        }

        synchronized (lock) {
            knownHandlers.put(serviceId, topicList);
            idMap.put(serviceId, handler);
        
            for(String s : topicList) {
            	Map<String, Map<T, Filter>> mapToUse;
            	String topicToUse;
            	if(isWildcard(s)) {
            		mapToUse = wildcardMap;
            		topicToUse = s.length() == 1 ? "" : s.substring(0, s.length() - 2);
            	} else {
            		mapToUse = map;
            		topicToUse = s;
            	}
            	Map<T, Filter> handlers = mapToUse.computeIfAbsent(topicToUse, x1 -> new HashMap<>());
            	handlers.put(handler, f);
            }
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
        synchronized (lock) {
            List<String> consumed = knownHandlers.remove(serviceId);
            idMap.remove(serviceId);
            if (consumed != null) {
                consumed.forEach(s -> {
                	Map<String, Map<T, U>> handlers;
                	String key;
                	if(isWildcard(s)) {
                		handlers = wildcardMap;
                		key = s.length() == 1 ? "" : s.substring(0, s.length() - 2);
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
            }
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

    private <T> void doUpdatedEventHandler(Map<String, Map<T, Filter>> map, Map<String, Map<T, Filter>> wildcardMap, Map<Long,T> idToHandler, String defaultTopic,
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
        deliver(topic, EventConverter.forTypedEvent(event));
    }

    @Override
    public void deliverUntyped(String topic, Map<String, ?> eventData) {
    	checkTopicSyntax(topic);
    	Objects.requireNonNull(eventData, "The event object must not be null");
        deliver(topic, EventConverter.forUntypedEvent(eventData));
    }

    private void deliver(String topic, EventConverter convertibleEventData) {

        List<EventTask> deliveryTasks;

        synchronized (lock) {
        	List<EventTask> typedDeliveries = toTypedEventTasks(
        			topicsToTypedHandlers.getOrDefault(topic, emptyMap()), topic, convertibleEventData);

            List<EventTask> untypedDeliveries = toUntypedEventTasks(
            		topicsToUntypedHandlers.getOrDefault(topic, emptyMap()), topic, convertibleEventData);

            List<EventTask> wildcardDeliveries = new ArrayList<>();
            String truncatedTopic = topic;
            do {
            	int idx = truncatedTopic.lastIndexOf('/');
            	truncatedTopic = idx > 0 ? truncatedTopic.substring(0, idx) : "";
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
                // TODO log properly
                System.out.println("Unhandled Event Handlers are being used for event sent to topic " + topic);
                deliveryTasks = unhandledEventHandlers.stream()
                        .map(handler -> new UnhandledEventTask(topic, convertibleEventData, handler)).collect(toList());
            }
        }

        queue.add(new MonitorEventTask(topic, convertibleEventData, monitorImpl));

        queue.addAll(deliveryTasks);
    }
    
    private List<EventTask> toTypedEventTasks(Map<TypedEventHandler<?>, Filter> map, 
    		String topic, EventConverter convertibleEventData) {
    	List<EventTask> list = new ArrayList<>();
    	for(Entry<TypedEventHandler<?>, Filter> e : map.entrySet()) {
    		Filter f = e.getValue();
    		if(f == null || convertibleEventData.applyFilter(f)) {
    			TypedEventHandler<?> handler = e.getKey();
    			list.add(new TypedEventTask(topic, convertibleEventData, handler,
                typedHandlersToTargetClasses.get(handler)));
    		}
    	}
    	return list;
    }

    private List<EventTask> toUntypedEventTasks(Map<UntypedEventHandler, Filter> map, 
    		String topic, EventConverter convertibleEventData) {
    	List<EventTask> list = new ArrayList<>();
    	for(Entry<UntypedEventHandler, Filter> e : map.entrySet()) {
    		Filter f = e.getValue();
    		if(f == null || convertibleEventData.applyFilter(f)) {
    			UntypedEventHandler handler = e.getKey();
    			list.add(new UntypedEventTask(topic, convertibleEventData, handler));
    		}
    	}
    	return list;
    }

    private static void checkTopicSyntax(String topic) {
    	String msg = checkTopicSyntax(topic, false);
    	if(msg != null) {
    		throw new IllegalArgumentException(msg);
    	}
    }
    
    private static String checkTopicSyntax(String topic, boolean wildcardPermitted) {
    	
    	if(topic == null) {
    		throw new IllegalArgumentException("The topic name is not permitted to be null");
    	}
    	
    	boolean slashPermitted = false;
    	for(int i = 0; i < topic.length(); i++) {
    		int c = topic.codePointAt(i);
    		if('*' == c) {
    			if(!wildcardPermitted) {
    				return "Wildcard topics may not be used for sending events";
    			}
    			if(topic.length() != i + 1) {
    				return "The wildcard * is only permitted at the end of the topic";
    			}
    			if(topic.length() > 1 && topic.codePointAt(i - 1) != '/') {
    				return "The wildcard must be preceded by a / unless it is the only character in the topic string";
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
    
    private static boolean isWildcard(String topic) {
    	return topic.equals("*") || topic.endsWith("/*");
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
                    // TODO log the interrupt and continue
                    e.printStackTrace();
                    continue;
                }

                take.notifyListener();
            }

        }

    }
}
