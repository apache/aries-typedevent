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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.osgi.annotation.bundle.Capability;
import org.osgi.namespace.service.ServiceNamespace;
import org.osgi.service.typedevent.monitor.MonitorEvent;
import org.osgi.service.typedevent.monitor.RangePolicy;
import org.osgi.service.typedevent.monitor.TypedEventMonitor;
import org.osgi.util.function.Predicate;
import org.osgi.util.pushstream.PushEvent;
import org.osgi.util.pushstream.PushEventConsumer;
import org.osgi.util.pushstream.PushEventSource;
import org.osgi.util.pushstream.PushStream;
import org.osgi.util.pushstream.PushStreamProvider;
import org.osgi.util.pushstream.PushbackPolicyOption;
import org.osgi.util.pushstream.QueuePolicyOption;
import org.osgi.util.pushstream.SimplePushEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Capability(namespace = ServiceNamespace.SERVICE_NAMESPACE, attribute = "objectClass:List<String>=org.osgi.service.typedevent.monitor.TypedEventMonitor", uses = TypedEventMonitor.class)
public class TypedEventMonitorImpl implements TypedEventMonitor {

	private static final Logger _log = LoggerFactory.getLogger(TypedEventMonitorImpl.class);
	
    private final LinkedList<MonitorEvent> historicEvents = new LinkedList<MonitorEvent>();

    private final ExecutorService monitoringWorker;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final PushStreamProvider psp;

    private final SimplePushEventSource<MonitorEvent> source;

    private final int historySize = 1024;
    
    private final SortedMap<EventSelector, RangePolicy> historyConfiguration = new TreeMap<>();
    
    private final Map<String, TopicHistory> topicsWithRestrictedHistories = new HashMap<>();

    TypedEventMonitorImpl(Map<String, ?> props) {

    	Object object = props.get("event.history.enable.at.start");
    	if(object == null || "true".equals(object.toString())) {
    		historyConfiguration.put(new EventSelector("*", null), RangePolicy.unlimited());
    	}
    	
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

    	MonitorEvent me = null;
        lock.writeLock().lockInterruptibly();
        try {
        	RangePolicy policy = doGetEffectiveHistoryStorage(topic);
        	
        	if(policy.getMaximum() > 0) {
				me = getMonitorEvent(topic, eventData);
        		
        		historicEvents.addFirst(me);

        		if(policy.getMaximum() < historySize) {
        			TopicHistory th = topicsWithRestrictedHistories.computeIfAbsent(topic, 
        					t -> new TopicHistory(policy.getMinimum(), policy.getMaximum()));
        			MonitorEvent old = th.addEvent(me);
        			if(old != null) {
        				historicEvents.remove(old);
        			}
        		}
        		

        		int toRemove = historicEvents.size() - historySize;
        		if(toRemove > 0) {
        			Iterator<MonitorEvent> it = historicEvents.descendingIterator();
        			for (; toRemove > 0 && it.hasNext();) {
        				MonitorEvent toCheck = it.next();
        				TopicHistory th = topicsWithRestrictedHistories.get(toCheck.topic);
        				if(th == null || th.clearEvent(toCheck)) {
        					it.remove();
        					toRemove--;
        				}
        			}
        		}
        	}
        } finally {
        	lock.writeLock().unlock();
        }
        
        if(source.isConnected()) {
        	source.publish(me == null? getMonitorEvent(topic, eventData) : me);
        }
    }

	private MonitorEvent getMonitorEvent(String topic, Map<String, Object> eventData) {
		MonitorEvent me = new MonitorEvent();
		me.eventData = eventData;
		me.topic = topic;
		me.publicationTime = Instant.now();
		return me;
	}

    @Override
    public PushStream<MonitorEvent> monitorEvents() {
        return monitorEvents(0);
    }

    @Override
    public PushStream<MonitorEvent> monitorEvents(int history) {
    	return monitorEvents(history, false);
    }
    
    @Override
    public PushStream<MonitorEvent> monitorEvents(int history, boolean historyOnly) {
        return psp.buildStream(eventSource(history, historyOnly))
                .withBuffer(new ArrayBlockingQueue<>(Math.max(historySize, history)))
                .withPushbackPolicy(PushbackPolicyOption.FIXED, 0).withQueuePolicy(QueuePolicyOption.FAIL)
                .withExecutor(monitoringWorker).build();
    }

    @Override
    public PushStream<MonitorEvent> monitorEvents(Instant history) {
    	return monitorEvents(history, false);
    }
    
    @Override
    public PushStream<MonitorEvent> monitorEvents(Instant history, boolean historyOnly) {
        return psp.buildStream(eventSource(history, historyOnly)).withBuffer(new ArrayBlockingQueue<>(1024))
                .withPushbackPolicy(PushbackPolicyOption.FIXED, 0).withQueuePolicy(QueuePolicyOption.FAIL)
                .withExecutor(monitoringWorker).build();
    }

    PushEventSource<MonitorEvent> eventSource(int events, boolean historyOnly) {

        return pec -> {
        	List<MonitorEvent> list;

        	lock.readLock().lockInterruptibly();
        	try {
        		int toSend = Math.min(historicEvents.size(), events);
        		list = new ArrayList<>(historicEvents.subList(0, toSend));
        	} finally {
        		lock.readLock().unlock();
        	}
        	return pushBackwards(pec, list, historyOnly);
        };
    }

    PushEventSource<MonitorEvent> eventSource(Instant since, boolean historyOnly) {

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
        	return pushBackwards(pec, list, historyOnly);
        };
    }

	private AutoCloseable pushBackwards(PushEventConsumer<? super MonitorEvent> pec, List<MonitorEvent> list, boolean historyOnly)
			throws Exception {
		ListIterator<MonitorEvent> li = list.listIterator(list.size());
		while (li.hasPrevious()) {
			try {
				if (pec.accept(PushEvent.data(li.previous())) < 0) {
					if(_log.isDebugEnabled()) {
                		_log.debug("Historical event delivery halted by the consumer");
                	}
					return () -> {
					};
				}
			} catch (Exception e) {
				_log.warn("An error occurred delivering historical events", e);
				return () -> {
				};
			}
		}
		if(historyOnly) {
			try {
			pec.accept(PushEvent.close());
			} catch (Exception e) {}
			return () -> {};
		} else {
			return source.open(pec);
		}
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

	@Override
	public Predicate<String> topicFilterMatches(String topicFilter) {
		TypedEventBusImpl.checkTopicSyntax(topicFilter, true);
		EventSelector selector = new EventSelector(topicFilter, null);
		return selector::matchesTopic;
	}

	@Override
	public boolean topicFilterMatches(String topicName, String topicFilter) {
		TypedEventBusImpl.checkTopicSyntax(topicFilter, true);
		TypedEventBusImpl.checkTopicSyntax(topicName);
		EventSelector selector = new EventSelector(topicFilter, null);
		return selector.matchesTopic(topicName);
	}

	@Override
	public int getMaximumEventStorage() {
		return historySize;
	}

	@Override
	public Map<String, RangePolicy> getConfiguredHistoryStorage() {
		Map<String, RangePolicy> copy = new LinkedHashMap<>();
		lock.readLock().lock();
		try {
			for (Entry<EventSelector, RangePolicy> e : historyConfiguration.entrySet()) {
				copy.put(e.getKey().getTopicFilter(), e.getValue());
			}
		} finally {
			lock.readLock().unlock();
		}
		return copy;
	}

	@Override
	public RangePolicy getConfiguredHistoryStorage(String topicFilter) {
		TypedEventBusImpl.checkTopicSyntax(topicFilter, true);
		EventSelector selector = new EventSelector(topicFilter, null);
		lock.readLock().lock();
		try {
			return historyConfiguration.get(selector);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public RangePolicy getEffectiveHistoryStorage(String topicName) {
		TypedEventBusImpl.checkTopicSyntax(topicName);
		lock.readLock().lock();
		try {
			return doGetEffectiveHistoryStorage(topicName);
		} finally {
			lock.readLock().unlock();
		}
	}

	private RangePolicy doGetEffectiveHistoryStorage(String topicName) {
		return historyConfiguration.entrySet().stream()
				.filter(e -> e.getKey().matchesTopic(topicName))
				.map(Entry::getValue)
				.findFirst()
				.orElse(RangePolicy.none());
	}

	@Override
	public int configureHistoryStorage(String topicFilter, RangePolicy policy) {
		
		if(policy.getMinimum() > 0) {
			TypedEventBusImpl.checkTopicSyntax(topicFilter);
		} else {
			TypedEventBusImpl.checkTopicSyntax(topicFilter, true);
		}
		
		EventSelector key = new EventSelector(topicFilter, null);
		int min = policy.getMinimum();
		int max = policy.getMaximum();
		long available;
		lock.writeLock().lock();
		try {
			available = historySize - historyConfiguration.entrySet().stream()
					.filter(e -> !e.getKey().getTopicFilter().equals(topicFilter))
					.mapToInt(e -> e.getValue().getMinimum()).sum();
			if(available < min) {
				throw new IllegalStateException("Insufficient space available for " + min + " events");
			}
			
			RangePolicy old = historyConfiguration.put(key, policy);
			if(key.isWildcard()) {
				if(old == null || old.getMinimum() != min || old.getMaximum() != max) {
					
					Consumer<String> action = (min > 0 || max < historySize) ?
							s -> updateRestrictedHistory(s, min, max) :
							topicsWithRestrictedHistories::remove;
					for(Entry<String, TopicHistory> e : topicsWithRestrictedHistories.entrySet()) {
						if(key.matchesTopic(e.getKey())) {
							RangePolicy effectivePolicy = getEffectiveHistoryStorage(e.getKey());
							if(!e.getValue().policyMatches(effectivePolicy)) {
								action.accept(e.getKey());
							}
						}
					}
				}
			} else if(min > 0 || max < historySize){
				updateRestrictedHistory(topicFilter, min, max);
			} else {
				topicsWithRestrictedHistories.remove(topicFilter);
			}
		} finally {
			lock.writeLock().unlock();
		}
		return (int) Math.min(max, available);
	}

	private void updateRestrictedHistory(String topicFilter, int minRequired, int maxRequired) {
		TopicHistory newHistory = new TopicHistory(minRequired, maxRequired);
		TopicHistory oldHistory = topicsWithRestrictedHistories.put(topicFilter, newHistory);
		if(oldHistory != null) {
			List<MonitorEvent> toRemove = newHistory.copyFrom(oldHistory);
			historicEvents.removeAll(toRemove);
		}
	}

	@Override
	public void removeHistoryStorage(String topicFilter) {
		EventSelector selector = new EventSelector(topicFilter, null);
		lock.readLock().lock();
		try {
			historyConfiguration.remove(selector);
		} finally {
			lock.readLock().unlock();
		}
	}
}
