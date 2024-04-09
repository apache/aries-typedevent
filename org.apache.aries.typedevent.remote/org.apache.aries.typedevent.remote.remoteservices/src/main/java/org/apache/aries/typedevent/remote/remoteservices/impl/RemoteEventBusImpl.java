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
package org.apache.aries.typedevent.remote.remoteservices.impl;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.aries.typedevent.remote.api.RemoteEventConstants.RECEIVE_REMOTE_EVENTS;
import static org.osgi.namespace.service.ServiceNamespace.SERVICE_NAMESPACE;
import static org.osgi.util.converter.Converters.standardConverter;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.aries.typedevent.remote.api.RemoteEventConstants;
import org.apache.aries.typedevent.remote.remoteservices.spi.RemoteEventBus;
import org.osgi.annotation.bundle.Capability;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.Filter;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.typedevent.TypedEventBus;
import org.osgi.service.typedevent.annotations.RequireTypedEvent;

/**
 * This class implements {@link RemoteEventBus} and is responsible for receiving
 * events from remote frameworks and publishing them in the local framework
 */
@Capability(namespace=SERVICE_NAMESPACE, attribute="objectClass:List<String>=org.apache.aries.typedevent.remote.remoteservices.spi.RemoteEventBus", uses=RemoteEventBus.class)
@RequireTypedEvent
public class RemoteEventBusImpl implements RemoteEventBus {
    
    private final TypedEventBus eventBus;
    
    private ServiceRegistration<RemoteEventBus> reg;
    
    private Map<String, Filter> topicsToFilters = new HashMap<>();
    
    private final Map<Long, Map<String, Filter>> servicesToInterests = new HashMap<>();
    
    private final Object lock = new Object();
    
    private final Config configuration;
    
    public RemoteEventBusImpl(TypedEventBus eventBus, Map<String, ?> config) {
        this.eventBus = eventBus;

        Map<String, Object> configWithDefaults = new HashMap<String, Object>(config);
        configWithDefaults.putIfAbsent("listener.selection", Config.Selector.WITH_PROPERTY);
        
        this.configuration = standardConverter().convert(configWithDefaults).to(Config.class);
    }
    
    public void init(BundleContext ctx) {
        ServiceRegistration<RemoteEventBus> reg = ctx.registerService(RemoteEventBus.class, this, null);
        
        Map<String, Filter> filters;
        synchronized(lock) {
            this.reg = reg;
            filters = topicsToFilters;
        }
        updateReg(filters);
    }

    public void destroy() {
        try {
            ServiceRegistration<?> reg;
            synchronized (lock) {
                reg = this.reg;
                this.reg = null;
            }
            
            if(reg != null) {
                reg.unregister();
            }
        } catch (IllegalStateException ise) {
            // TODO log
        }
    }
    
    @Override
    public void notify(String topic, Map<String, Object> properties) {
        
        boolean hasTopicInterest;
        Filter filter;
        synchronized (lock) {
            hasTopicInterest = topicsToFilters.containsKey(topic);
            filter = topicsToFilters.get(topic);
        }
        
        if(hasTopicInterest) {
            if(filter == null || filter.matches(properties)) {
                properties.put(RemoteEventConstants.REMOTE_EVENT_MARKER, Boolean.TRUE);
                eventBus.deliverUntyped(topic, properties);
            } else {
                //TODO log filter mismatch
            }
        } else {
            // TODO log topic mismatch
        }
    }

    /**
     * Update the data structures and registration to reflect the topic interests
     * of the local framework
     * 
     * @param id
     * @param topics
     * @param filter
     */
    void updateLocalInterest(Long id, List<String> topics, Filter filter, Map<String, ?> serviceProps) {

        Map<String, Filter> newData;
        Supplier<Map<String, Filter>> fromTopics = () -> topics.stream()
                .collect(toMap(identity(), x -> filter, (a,b) -> a));
        
        switch(configuration.listener_selection()) {
            case ALL:
                newData = fromTopics.get();
                break;
            case CUSTOM:
                String listenerFilterString = configuration.listener_selection_custom_filter();
                try {
                    Filter listenerFilter = FrameworkUtil.createFilter(listenerFilterString);
                    
                    if(listenerFilter.matches(serviceProps)) {
                        newData = fromTopics.get();
                        break;
                    }
                } catch (InvalidSyntaxException ise) {
                    //TODO log that this is ignored;
                }
                newData = new HashMap<>();
                break;
            case WITH_FILTER:
                newData = filter == null ? new HashMap<>() : fromTopics.get();
                break;
            case WITH_PROPERTY:
                boolean hasProperty = Boolean.valueOf(String.valueOf(serviceProps.get(RECEIVE_REMOTE_EVENTS)));
                newData = hasProperty ? fromTopics.get() : new HashMap<>();
                break;
            default:
                newData = new HashMap<>();
                break;
        
        }
        
        boolean doUpdate;
        Map<String, Filter> updatedFilters;
        synchronized(lock) {
            servicesToInterests.put(id, newData);
            
            Map<String, Filter> tmpFilters = topicsToFilters;
            topicsToFilters = getUpdatedFilters();
            
            doUpdate = !tmpFilters.equals(topicsToFilters);
            
            updatedFilters = topicsToFilters;
        }
        
        if(doUpdate) {
            updateReg(updatedFilters);
        }
    }

    private Map<String, Filter> getUpdatedFilters() {
        synchronized (lock) {
            return servicesToInterests.values().stream()
                    .flatMap(m -> m.entrySet().stream())
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue, 
                            this::combineFilters));
        }
    }

    private Filter combineFilters(Filter a, Filter b) {
        if(a == null) {
            return b;
        } else if (b == null) {
            return a;
        } else {
            try {
                return FrameworkUtil.createFilter("(|" + a.toString() + b.toString() + ")");
            } catch (InvalidSyntaxException e) {
                // TODO Auto-generated catch block
                throw new RuntimeException(e);
            }
        }
    }
    
    private void updateReg(Map<String, Filter> filters) {
        
        Hashtable<String, Object> props = new Hashtable<>();
        
        props.put(Constants.SERVICE_EXPORTED_INTERFACES, RemoteEventBus.class.getName());
        props.put(Constants.SERVICE_EXPORTED_INTENTS, "osgi.basic");
        List<String> remoteFilters = filters.entrySet().stream()
                .map(e -> e.getKey() + "=" + (e.getValue() == null ? "" : e.getValue().toString()))
                .collect(toList());
        props.put(REMOTE_EVENT_FILTERS, remoteFilters);
        
        
        ServiceRegistration<?> reg;
        synchronized (lock) {
            reg = this.reg;
        }
        
        if(reg != null) {
            // Only update if there is a change
            Object existingFilters = reg.getReference().getProperty(REMOTE_EVENT_FILTERS);
            if(!remoteFilters.equals(existingFilters)) {
                reg.setProperties(props);
            }
            // Deal with a race condition if
            Map<String, Filter> updatedFilters;
            synchronized (lock) {
                updatedFilters = topicsToFilters;
            }
            if(!updatedFilters.equals(filters)) {
                updateReg(updatedFilters);
            }
        }
    }

    void removeLocalInterest(Long id) {

        Map<String, Filter> updatedFilters; 
        synchronized(lock) {
            if(servicesToInterests.remove(id) == null) {
                return;
            }
            topicsToFilters = getUpdatedFilters();
            updatedFilters = topicsToFilters;
        }
        
        updateReg(updatedFilters);
    }
}
