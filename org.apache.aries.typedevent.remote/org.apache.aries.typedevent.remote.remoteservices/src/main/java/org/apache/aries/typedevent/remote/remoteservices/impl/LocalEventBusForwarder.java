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

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.osgi.util.converter.Converters.standardConverter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.aries.typedevent.remote.remoteservices.spi.RemoteEventBus;
import org.apache.aries.typedevent.remote.spi.LocalEventConsumerManager;
import org.osgi.framework.Constants;
import org.osgi.framework.Filter;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.util.converter.TypeReference;

/**
 * This class is responsible for taking events from the local framework and
 * sending them on to interested remote frameworks
 */
public class LocalEventBusForwarder extends LocalEventConsumerManager {

    private static final TypeReference<List<String>> LIST_OF_STRINGS = new TypeReference<List<String>>() {
    };
    
    /**
     * Map access and mutation must be synchronized on {@link #lock}. Values from
     * the map should be copied as the contents are not thread safe.
     */
    private final Map<String, Map<RemoteEventBus, Filter>> eventTypeToRemotes = new HashMap<>();
    
    /**
     * Map access and mutation must be synchronized on {@link #lock}.
     * Values from the map should be copied as the contents are not thread safe.
     */
    private final Map<Long, List<String>> remoteTopicInterests = new HashMap<>();

    /**
     * Map access and mutation must be synchronized on {@link #lock}.
     * Values from the map should be copied as the contents are not thread safe.
     */
    private final Map<Long, RemoteEventBus> remoteBuses = new HashMap<>();
    
    private final Object lock = new Object();

    @Override
    public void notifyUntyped(String topic, Map<String, Object> event) {
        Map<RemoteEventBus, Filter> possibleTargets;
        synchronized (lock) {
            possibleTargets = eventTypeToRemotes.getOrDefault(topic, emptyMap());
        }
        
        possibleTargets.entrySet().stream()
            .filter(e -> e.getValue() == null || e.getValue().matches(event))
            .map(Entry::getKey)
            .forEach(r -> r.notify(topic, event));
    }

    private Long getServiceId(Map<String, Object> properties) {
        return standardConverter().convert(properties.get(Constants.SERVICE_ID)).to(Long.class);
    }

    void addRemoteEventBus(RemoteEventBus remote, Map<String, Object> properties) {
        doAdd(remote, properties);
        updateRemoteInterest();
    }

    private void doAdd(RemoteEventBus remote, Map<String, Object> properties) {
        Object consumed = properties.get(RemoteEventBus.REMOTE_EVENT_FILTERS);

        if (consumed == null) {
            // TODO log a broken behaviour
            return;
        }

        Map<String, Filter> topicsToFilters = standardConverter().convert(consumed).to(LIST_OF_STRINGS)
                .stream()
                .map(s -> s.split("=", 2))
                .collect(toMap(s -> s[0], s -> safeCreateFilter(s[1])));

        Long serviceId = getServiceId(properties);

        List<String> interestedTopics = topicsToFilters.keySet().stream().collect(toList());
        synchronized (lock) {
            remoteBuses.put(serviceId, remote);
            remoteTopicInterests.put(serviceId, interestedTopics);
            
            interestedTopics.forEach(s -> {
                Map<RemoteEventBus, Filter> perTopicMap = eventTypeToRemotes
                        .computeIfAbsent(s, x -> new HashMap<>());
                perTopicMap.put(remote, topicsToFilters.get(s));
            });
        }
    }
    
    private Filter safeCreateFilter(String filterString) {
        try {
            return FrameworkUtil.createFilter(filterString);
        } catch (InvalidSyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            try {
                return FrameworkUtil.createFilter("(&(x=true)(x=false))");
            } catch (InvalidSyntaxException e1) {
                // TODO log properly
                throw new RuntimeException("Serious problem!");
            }
        }
    }

    void updatedRemoteEventBus(Map<String, Object> properties) {
        Long serviceId = getServiceId(properties);
        synchronized (lock) {
            RemoteEventBus remote = remoteBuses.get(serviceId);
            doRemove(remote, properties);
            doAdd(remote, properties);
        }
        updateRemoteInterest();
    }

    void removeRemoteEventBus(RemoteEventBus remote, Map<String, Object> properties) {
        doRemove(remote, properties);
        updateRemoteInterest();
    }

    private void doRemove(RemoteEventBus remote, Map<String, Object> properties) {
        Long serviceId = getServiceId(properties);

        synchronized (lock) {
            remoteBuses.remove(serviceId);
            List<String> consumed = remoteTopicInterests.remove(serviceId);
            if(consumed != null) {
                consumed.forEach(s -> {
                    Map<RemoteEventBus, ?> perTopic = eventTypeToRemotes.get(s);
                    if(perTopic != null) {
                        perTopic.remove(remote);
                        if(perTopic.isEmpty()) {
                            eventTypeToRemotes.remove(s);
                        }
                    }
                });
            }
        }
    }

    private void updateRemoteInterest() {

        Map<String, String> targets;
        synchronized (lock) {
            targets = eventTypeToRemotes.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, 
                    e -> e.getValue().values().stream()
                    .map(f -> f == null ? "" : f.toString())
                    .reduce("", this::mergeFilterStrings)));
            
        }
        
        updateTargets(targets);
    }
    
    private String mergeFilterStrings(String a, String b) {
        if(a == null || "".equals(a)) {
            return b == null ? "" : b;
        } else if (b == null || "".equals(b)) {
            return a;
        } else {
            return "(|" + a + b + ")";
        }
    }
}
