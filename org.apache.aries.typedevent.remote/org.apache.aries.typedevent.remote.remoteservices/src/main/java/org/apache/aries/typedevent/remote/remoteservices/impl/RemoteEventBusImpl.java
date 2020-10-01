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
import static org.osgi.namespace.service.ServiceNamespace.SERVICE_NAMESPACE;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.osgi.service.typedevent.annotations.RequireTypedEvents;

/**
 * This class implements {@link RemoteEventBus} and is responsible for receiving
 * events from remote frameworks and publishing them in the local framework
 */
@Capability(namespace=SERVICE_NAMESPACE, attribute="objectClass:List<String>=org.apache.aries.typedevent.remote.remoteservices.spi.RemoteEventBus", uses=RemoteEventBus.class)
@RequireTypedEvents
public class RemoteEventBusImpl implements RemoteEventBus {
    
    private final TypedEventBus eventBus;
    
    private ServiceRegistration<RemoteEventBus> reg;
    
    private Map<String, Filter> topicsToFilters = new HashMap<>();
    
    private final Map<Long, Map<String, Filter>> servicesToInterests = new HashMap<>();
    
    private final Object lock = new Object();

    public RemoteEventBusImpl(TypedEventBus eventBus) {
        this.eventBus = eventBus;
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
    void updateLocalInterest(Long id, List<String> topics, Filter filter) {

        boolean doUpdate = false;

        Map<String, Filter> newData = topics.stream()
                .collect(toMap(identity(), x -> filter, (a,b) -> a));
        
        Map<String, Filter> updatedFilters;
        synchronized(lock) {
            doUpdate = true;
            servicesToInterests.put(id, newData);
            topicsToFilters = getUpdatedFilters();
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
