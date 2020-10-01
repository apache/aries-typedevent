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
package org.apache.aries.typedevent.remote.spi;

import static java.lang.Boolean.TRUE;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_FILTER;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_TOPICS;

import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.typedevent.UntypedEventHandler;

/**
 * A simple helper class used to manage the registrations of {@link UntypedEventHandler}
 * services in the local service registry, used to feed events into the remote events
 * implementation.
 * 
 * Implementations should extend this type and override the {@link #notifyUntyped(String, Map)} method
 * to receive events. The set of events received can be altered by calling {@link #updateTargets(Map)}.
 */
public abstract class LocalEventConsumerManager implements UntypedEventHandler {
    
    /**
     * A service property indicating that the event handler is a proxy created for a remote node and so
     * should not be considered as a local interest.
     */
    public static final String ARIES_LOCAL_EVENT_PROXY = "org.apache.aries.typedevent.remote.spi.local.proxy";

    /**
     * A filter to exclude local proxy interests from remote nodes
     */
    public static final String ARIES_LOCAL_EVENT_PROXY_EXCLUSION_FILTER = "(!(" + ARIES_LOCAL_EVENT_PROXY + "=true))";
    
    private final Object lock = new Object();
    private final Map<String, ServiceRegistration<UntypedEventHandler>> listenerRegistrations = new HashMap<>();
    private final Map<String, String> topicsToFilters = new HashMap<>();
    private BundleContext ctx;
    
    /**
     * Starts this manager, registering any necessary whiteboard services with the
     * appropriate topic and filters;
     * @param ctx
     */
    public final void start(BundleContext ctx) {
        synchronized (lock) {
            this.ctx = ctx;
        }
        updateServiceRegistrations();
    }

    /**
     * Stops this manager, unregistering any whiteboard services
     */
    public final void stop() {
        synchronized (lock) {
            this.ctx = null;
        }
        Map<String, ServiceRegistration<UntypedEventHandler>> toUnregister;
        synchronized (lock) {
            toUnregister = new HashMap<>(listenerRegistrations);
            listenerRegistrations.clear();
        }
        toUnregister.values().stream().forEach(this::safeUnregister);
    }
    
    
    private void updateServiceRegistrations() {
        Map<String, String> possibleUpdates = new HashMap<String, String>();
        Map<String, ServiceRegistration<UntypedEventHandler>> toUnregister;
        synchronized (lock) {
            possibleUpdates = new HashMap<>(topicsToFilters);
            toUnregister = listenerRegistrations.entrySet().stream()
                    .filter(e -> !topicsToFilters.containsKey(e.getKey()))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
            listenerRegistrations.keySet().removeAll(toUnregister.keySet());
        }
        
        toUnregister.values().stream().forEach(this::safeUnregister);
        
        for (Entry<String, String> entry : possibleUpdates.entrySet()) {
            
            String topic = entry.getKey();
            String filter = entry.getValue();
            
            ServiceRegistration<UntypedEventHandler> reg;
            BundleContext ctx;
            synchronized (lock) {
                reg = listenerRegistrations.get(topic);    
                ctx = this.ctx;
            }
            
            if(reg == null) {
                if(ctx != null) {
                    Dictionary<String, Object> props = new Hashtable<>();
                    props.put(TYPED_EVENT_TOPICS, topic);
                    props.put(ARIES_LOCAL_EVENT_PROXY, TRUE);
                    if(filter != null && !filter.contentEquals("")) {
                        props.put(TYPED_EVENT_FILTER, filter);
                    }
                    reg = ctx.registerService(UntypedEventHandler.class, this, props);
                    
                    synchronized (lock) {
                        ServiceRegistration<UntypedEventHandler> oldReg = listenerRegistrations.putIfAbsent(topic, reg);
                        if(oldReg == null) {
                            reg = null;
                        }
                    }
                    if(reg != null) {
                        reg.unregister();
                    }
                }
            } else if(ctx != null) {
                
                Dictionary<String, Object> props = new Hashtable<>();
                props.put(TYPED_EVENT_TOPICS, topic);
                props.put(ARIES_LOCAL_EVENT_PROXY, TRUE);
                if(filter != null && !filter.contentEquals("")) {
                    if(filter.equals(reg.getReference().getProperty(TYPED_EVENT_FILTER))) {
                        // Filter unchanged - no need to update
                        continue;
                    }
                    props.put(TYPED_EVENT_FILTER, filter);
                } else if (reg.getReference().getProperty(TYPED_EVENT_FILTER) == null) {
                    // Filter unchanged - no need to update
                    continue;
                }
                reg.setProperties(props);
            }
        }
        
        boolean changed;
        synchronized (lock) {
            changed = !possibleUpdates.equals(topicsToFilters);
        }
        if(changed) {
            updateServiceRegistrations();
        }
    }
    
    private void safeUnregister(ServiceRegistration<?> reg) {
        try {
            reg.unregister();
        } catch (IllegalStateException ise) {
            // Just ignore it
        }
    }
    
    /**
     * Set the topic and filter targets for which whiteboard listeners
     * should be registered
     * @param updated - A Map of topic names (or globs) to filters
     */
    protected final void updateTargets(Map<String, String> updated) {
        synchronized (lock) {
            topicsToFilters.clear();
            topicsToFilters.putAll(updated);
        }
        
        updateServiceRegistrations();
    }
    
}
