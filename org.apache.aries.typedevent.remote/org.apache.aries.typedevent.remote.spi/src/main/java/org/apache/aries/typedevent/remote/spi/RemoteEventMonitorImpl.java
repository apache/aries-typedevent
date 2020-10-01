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

import static org.apache.aries.typedevent.remote.api.RemoteEventConstants.REMOTE_EVENT_MARKER;

import java.time.Instant;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.aries.typedevent.remote.api.FilterDTO;
import org.apache.aries.typedevent.remote.api.RemoteEventMonitor;
import org.apache.aries.typedevent.remote.api.RemoteMonitorEvent;
import org.apache.aries.typedevent.remote.api.RemoteMonitorEvent.PublishType;
import org.osgi.framework.Filter;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.typedevent.monitor.MonitorEvent;
import org.osgi.service.typedevent.monitor.TypedEventMonitor;
import org.osgi.util.converter.Converters;
import org.osgi.util.function.Predicate;
import org.osgi.util.pushstream.PushStream;

public class RemoteEventMonitorImpl implements RemoteEventMonitor {

    private final TypedEventMonitor monitor;

    public RemoteEventMonitorImpl(TypedEventMonitor monitor) {

        this.monitor = monitor;
    }

    private static RemoteMonitorEvent toRemoteEvent(MonitorEvent event) {

        Object remoteMarker = event.eventData.get(REMOTE_EVENT_MARKER);
        
        RemoteMonitorEvent me = Converters.standardConverter().convert(event).sourceAsDTO().targetAsDTO().to(RemoteMonitorEvent.class);
        me.publishType = Boolean.valueOf(String.valueOf(remoteMarker)) ? PublishType.REMOTE : PublishType.LOCAL;

        return me;
    }

    @Override
    public PushStream<RemoteMonitorEvent> monitorEvents(FilterDTO... filters) {
        return monitorEvents(0, filters);
    }

    @Override
    public PushStream<RemoteMonitorEvent> monitorEvents(int history, FilterDTO...filters) {
        return monitor.monitorEvents(history)
                .map(RemoteEventMonitorImpl::toRemoteEvent)
                .filter(createFilter(filters));
    }

    @Override
    public PushStream<RemoteMonitorEvent> monitorEvents(Instant history, FilterDTO...filters) {
        return monitor.monitorEvents(history)
                .map(RemoteEventMonitorImpl::toRemoteEvent)
                .filter(createFilter(filters));
    }

    private class FilterPair {
        Filter ldap;
        Pattern regex;

        FilterPair(FilterDTO filter) {
            if (filter.ldapExpression != null && !filter.ldapExpression.isEmpty()) {
                try {
                    ldap = FrameworkUtil.createFilter(filter.ldapExpression);
                } catch (InvalidSyntaxException e) {
                    throw new IllegalArgumentException(e);
                }
            }

            if (filter.regularExpression != null && !filter.regularExpression.isEmpty()) {
                regex = Pattern.compile(filter.regularExpression);
            }
        }
    }

    private Predicate<RemoteMonitorEvent> createFilter(FilterDTO... filters) {
        List<FilterPair> filterPairs = Arrays.asList(filters).stream()
                .map(FilterPair::new).collect(Collectors.toList());

        if (filterPairs.isEmpty()) {
            return x -> true;
        }

        return event -> {
            // We use a TreeMap to ensure predictable ordering of keys
            // This is important for the regex matching contract.

            SortedMap<String, Object> toFilter = new TreeMap<>();

            // Using a collector blew up with null values, even though they are
            // supported by the TreeMap
            event.eventData.entrySet().stream()
                    .flatMap(e -> flatten("", e))
                    .forEach(e -> toFilter.put(e.getKey(), e.getValue()));

            toFilter.put("-topic", event.topic);
            toFilter.put("-publishType", event.publishType);

            StringBuilder eventText = new StringBuilder();

            if (filterPairs.stream().anyMatch(p -> p.regex != null)) {
                toFilter.forEach((k, v) -> {
                    eventText.append(k).append(':').append(v).append(',');
                });
            }

            // If a FilterDTO contains both LDAP and regular expressions, then both must match.
            return filterPairs.stream().anyMatch(p ->
                    (p.ldap == null || p.ldap.matches(toFilter)) &&
                    (p.regex == null || p.regex.matcher(eventText).find())
            );
        };
    }

    private Stream<Entry<String, Object>> flatten(String parentScope,
            Entry<String, Object> entry) {

        if (entry.getValue() instanceof Map) {

            String keyPrefix = parentScope + entry.getKey() + ".";

            @SuppressWarnings("unchecked")
            Map<String, Object> subMap = (Map<String, Object>) entry.getValue();

            // Recursively flatten maps that are inside our map
            return subMap.entrySet().stream()
                .flatMap(e -> flatten(keyPrefix, e));
        } else if(parentScope.isEmpty()) {
            // Fast path for top-level entries
            return Stream.of(entry);
        } else {
            // Map the key of a nested entry into x.y.z
            return Stream.of(new AbstractMap.SimpleEntry<>(
                    parentScope + entry.getKey(), entry.getValue()));
        }

    }

}
