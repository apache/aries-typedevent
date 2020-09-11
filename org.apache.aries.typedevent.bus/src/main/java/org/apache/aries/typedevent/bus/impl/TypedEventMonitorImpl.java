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

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.osgi.annotation.bundle.Capability;
import org.osgi.namespace.service.ServiceNamespace;
import org.osgi.service.typedevent.monitor.MonitorEvent;
import org.osgi.service.typedevent.monitor.TypedEventMonitor;
import org.osgi.util.pushstream.PushEvent;
import org.osgi.util.pushstream.PushEventSource;
import org.osgi.util.pushstream.PushStream;
import org.osgi.util.pushstream.PushStreamProvider;
import org.osgi.util.pushstream.PushbackPolicyOption;
import org.osgi.util.pushstream.QueuePolicyOption;
import org.osgi.util.pushstream.SimplePushEventSource;

@Capability(namespace = ServiceNamespace.SERVICE_NAMESPACE, attribute = "objectClass:List<String>=org.osgi.service.typedevent.monitor.TypedEventMonitor", uses = TypedEventMonitor.class)
public class TypedEventMonitorImpl implements TypedEventMonitor {

    private final LinkedList<MonitorEvent> historicEvents = new LinkedList<MonitorEvent>();

    private final ExecutorService monitoringWorker;

    private final Object lock = new Object();

    private final PushStreamProvider psp;

    private final SimplePushEventSource<MonitorEvent> source;

    private final int historySize = 1024;

    public TypedEventMonitorImpl(Map<String, ?> props) {

        monitoringWorker = Executors.newCachedThreadPool();

        psp = new PushStreamProvider();
        source = psp.buildSimpleEventSource(MonitorEvent.class).withExecutor(monitoringWorker)
                .withQueuePolicy(QueuePolicyOption.BLOCK).build();
    }

    public void destroy() {
        source.close();
        monitoringWorker.shutdown();
    }

    public void event(String topic, Map<String, Object> eventData) {
        MonitorEvent me = new MonitorEvent();
        me.eventData = eventData;
        me.topic = topic;
        me.publicationTime = Instant.now();

        synchronized (lock) {
            historicEvents.add(me);
            int toRemove = historicEvents.size() - historySize;
            for (; toRemove > 0; toRemove--) {
                historicEvents.poll();
            }
            source.publish(me);
        }
    }

    @Override
    public PushStream<MonitorEvent> monitorEvents() {
        return monitorEvents(0);
    }

    @Override
    public PushStream<MonitorEvent> monitorEvents(int history) {
        return psp.buildStream(eventSource(history))
                .withBuffer(new ArrayBlockingQueue<>(Math.max(historySize, history)))
                .withPushbackPolicy(PushbackPolicyOption.FIXED, 0).withQueuePolicy(QueuePolicyOption.FAIL)
                .withExecutor(monitoringWorker).build();
    }

    @Override
    public PushStream<MonitorEvent> monitorEvents(Instant history) {
        return psp.buildStream(eventSource(history)).withBuffer(new ArrayBlockingQueue<>(1024))
                .withPushbackPolicy(PushbackPolicyOption.FIXED, 0).withQueuePolicy(QueuePolicyOption.FAIL)
                .withExecutor(monitoringWorker).build();
    }

    PushEventSource<MonitorEvent> eventSource(int events) {

        return pec -> {
            synchronized (lock) {

                int size = historicEvents.size();
                int start = Math.max(0, size - events);

                List<MonitorEvent> list = historicEvents.subList(start, size);

                for (MonitorEvent me : list) {
                    try {
                        if (pec.accept(PushEvent.data(me)) < 0) {
                            return () -> {
                            };
                        }
                    } catch (Exception e) {
                        return () -> {
                        };
                    }
                }
                return source.open(pec);
            }

        };
    }

    PushEventSource<MonitorEvent> eventSource(Instant since) {

        return pec -> {
            synchronized (lock) {

                ListIterator<MonitorEvent> it = historicEvents.listIterator();

                while (it.hasNext()) {
                    MonitorEvent next = it.next();
                    if (next.publicationTime.isAfter(since)) {
                        it.previous();
                        break;
                    }
                }

                while (it.hasNext()) {
                    try {
                        if (pec.accept(PushEvent.data(it.next())) < 0) {
                            return () -> {
                            };
                        }
                    } catch (Exception e) {
                        return () -> {
                        };
                    }
                }
                return source.open(pec);
            }

        };
    }

}
