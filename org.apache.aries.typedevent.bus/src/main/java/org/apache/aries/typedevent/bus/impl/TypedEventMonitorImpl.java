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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

import org.osgi.annotation.bundle.Capability;
import org.osgi.namespace.service.ServiceNamespace;
import org.osgi.service.typedevent.monitor.MonitorEvent;
import org.osgi.service.typedevent.monitor.TypedEventMonitor;
import org.osgi.util.pushstream.PushEvent;
import org.osgi.util.pushstream.PushEventConsumer;
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

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final PushStreamProvider psp;

    private final SimplePushEventSource<MonitorEvent> source;

    private final int historySize = 1024;

    TypedEventMonitorImpl(Map<String, ?> props) {

        monitoringWorker = Executors.newCachedThreadPool();

        psp = new PushStreamProvider();
        source = psp.buildSimpleEventSource(MonitorEvent.class).withExecutor(monitoringWorker)
                .withQueuePolicy(QueuePolicyOption.BLOCK).build();
    }

    void destroy() {
        source.close();
        monitoringWorker.shutdown();
    }

    void event(String topic, Map<String, Object> eventData) throws InterruptedException {
        MonitorEvent me = new MonitorEvent();
        me.eventData = eventData;
        me.topic = topic;
        me.publicationTime = Instant.now();

        lock.writeLock().lockInterruptibly();
        try {
        	historicEvents.addFirst(me);
        	int toRemove = historicEvents.size() - historySize;
        	for (; toRemove > 0; toRemove--) {
        		historicEvents.removeLast();
        	}
        } finally {
        	lock.writeLock().unlock();
        }
        source.publish(me);
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
        	List<MonitorEvent> list;

        	lock.readLock().lockInterruptibly();
        	try {
        		int toSend = Math.min(historicEvents.size(), events);
        		list = new ArrayList<>(historicEvents.subList(0, toSend));
        	} finally {
        		lock.readLock().unlock();
        	}
        	return pushBackwards(pec, list);
        };
    }

    PushEventSource<MonitorEvent> eventSource(Instant since) {

        return pec -> {
        	List<MonitorEvent> list = new ArrayList<>();
        	lock.readLock().lockInterruptibly();
        	try {
        		Iterator<MonitorEvent> it = historicEvents.iterator();
        		while(it.hasNext()) {
    				MonitorEvent next = it.next();
        			if (!next.publicationTime.isAfter(since)) {
        				break;
        			} else {
        				list.add(next);
        			}
        		}
        	} finally {
        		lock.readLock().unlock();
        	}
        	return pushBackwards(pec, list);
        };
    }

	private AutoCloseable pushBackwards(PushEventConsumer<? super MonitorEvent> pec, List<MonitorEvent> list)
			throws Exception {
		ListIterator<MonitorEvent> li = list.listIterator(list.size());
		while (li.hasPrevious()) {
			try {
				if (pec.accept(PushEvent.data(li.previous())) < 0) {
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

    <T> T copyOfHistory(Function<Stream<MonitorEvent>, T> events) {
    	lock.readLock().lock();
    	try {
    		Stream<MonitorEvent> s = historicEvents.stream();
    		T t = events.apply(s);
    		s.close();
    		return t;
    	} finally {
    		lock.readLock().unlock();
    	}
    }
}
