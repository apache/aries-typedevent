/*******************************************************************************
 * Copyright (c) Contributors to the Eclipse Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0 
 *******************************************************************************/
package org.osgi.service.typedevent.monitor;

import java.time.Instant;
import java.util.Map;

import org.osgi.annotation.versioning.ProviderType;
import org.osgi.util.function.Predicate;
import org.osgi.util.pushstream.PushStream;

/**
 * The EventMonitor service can be used to monitor the events that are sent
 * using the EventBus, and that are received from remote EventBus instances
 * 
 * @ThreadSafe
 * @author $Id: 05a263f6544754849171431982a20dae9a2c2da4 $
 */
@ProviderType
public interface TypedEventMonitor {

    /**
	 * Get a stream of events, starting now.
	 *
	 * @return A stream of event data
	 */
	PushStream<MonitorEvent> monitorEvents();

	/**
	 * Get a stream of events, including up to the requested number of
	 * historical data events.
	 * <p>
	 * Logically equivalent to <code>monitorEvents(history, false)</code>.
	 *
	 * @param history The requested number of historical events, note that fewer
	 *            than this number of events may be returned if history is
	 *            unavailable, or if insufficient events have been sent.
	 * @return A stream of event data
	 */
	PushStream<MonitorEvent> monitorEvents(int history);

	/**
	 * Get a stream of events, including up to the requested number of
	 * historical data events.
	 *
	 * @param history The requested number of historical events, note that fewer
	 *            than this number of events may be returned if history is
	 *            unavailable, or if insufficient events have been sent.
	 * @param historyOnly If <code>true</code> then the returned stream will be
	 *            closed as soon as the available history has been delivered
	 * @return A stream of event data
	 * @since 1.1
	 */
	PushStream<MonitorEvent> monitorEvents(int history, boolean historyOnly);

	/**
	 * Get a stream of events, including historical data events prior to the
	 * supplied time.
	 * <p>
	 * Logically equivalent to <code>monitorEvents(history, false)</code>.
	 *
	 * @param history The requested time after which historical events, should
	 *            be included. Note that events may have been discarded, or
	 *            history unavailable.
	 * @return A stream of event data
	 */
	PushStream<MonitorEvent> monitorEvents(Instant history);

	/**
	 * Get a stream of events, including historical data events prior to the
	 * supplied time.
	 *
	 * @param history The requested time after which historical events, should
	 *            be included. Note that events may have been discarded, or
	 *            history unavailable.
	 * @param historyOnly If <code>true</code> then the returned stream will be
	 *            closed as soon as the available history has been delivered
	 * @return A stream of event data
	 * @since 1.1
	 */
	PushStream<MonitorEvent> monitorEvents(Instant history,
			boolean historyOnly);

	/**
	 * Get a Predicate which will match the supplied topic filter against a
	 * topic name.
	 * 
	 * @param topicFilter The topic filter to match against
	 * @return A predicate that will return true if the topic name being tested
	 *         matches the supplied topic filter.
	 * @since 1.1
	 * @throws NullPointerException if the topic filter is <code>null</code>
	 * @throws IllegalArgumentException if the topic filter contains invalid
	 *             syntax
	 */
	Predicate<String> topicFilterMatches(String topicFilter);

	/**
	 * Test the supplied topic filter against the supplied topic name.
	 * 
	 * @param topicName The topic name to match against
	 * @param topicFilter The topic filter to match against
	 * @return A predicate that will return true if the topic name being tested
	 *         matches the supplied topic filter.
	 * @since 1.1
	 * @throws NullPointerException if the topic filter is <code>null</code>
	 * @throws IllegalArgumentException if the topic filter or topic name
	 *             contain invalid syntax
	 */
	boolean topicFilterMatches(String topicName, String topicFilter);

	/**
	 * Get an estimate of the maximum number of historic events that can be
	 * stored by the TypedEvent implementation. If there is no fixed limit then
	 * -1 is returned. If no history storage is supported then zero is returned.
	 * 
	 * @return The maximum number of historic events that can be stored.
	 * @since 1.1
	 */
	int getMaximumEventStorage();

	/**
	 * Get the configured history storage for the Typed Events implementation.
	 * <p>
	 * The returned {@link Map} uses topic filter strings as keys. These filter
	 * strings may contain wildcards. If multiple filter strings are able to
	 * match then the most specific match applies with the following ordering:
	 * <ol>
	 * <li>An exact topic match</li>
	 * <li>An exact match of the parent topic and a single level wildcard as the
	 * final token</li>
	 * <li>An exact match of the parent topic and multi-level wildcard as the
	 * final token</li>
	 * </ol>
	 * <p>
	 * This ordering is applied recursively starting with the first topic token
	 * until only one candidate remains. The keys in the returned map are
	 * ordered such that the first encountered key which matches a given topic
	 * name is the configuration that will apply to that topic name.
	 * <p>
	 * The value associated with each key is the {@link RangePolicy} defining
	 * the number of events that will be stored.
	 *
	 * @return The configured history storage
	 * @since 1.1
	 */
	Map<String,RangePolicy> getConfiguredHistoryStorage();

	/**
	 * Get the configured history storage for a given topic filter. This method
	 * looks for an exact match in the history configuration. If no
	 * configuration is found for the supplied topic filter then
	 * <code>null</code> is returned.
	 * 
	 * @param topicFilter the topic filter
	 * @return The {@link RangePolicy} defining the number of stored events for
	 *         the topic. If no configuration is set for the topic filter then
	 *         <code>null</code> will be returned.
	 * @throws NullPointerException if the topic filter is <code>null</code>
	 * @throws IllegalArgumentException if the topic filter contains invalid
	 *             syntax
	 * @since 1.1
	 */
	RangePolicy getConfiguredHistoryStorage(String topicFilter);

	/**
	 * Get the history storage rule that applies to a given topic name. This
	 * method takes into account the necessary precedence rules to find the
	 * correct configuration for the named method, and so will never return
	 * <code>null</code>.
	 * 
	 * @param topicName the topic name
	 * @return The {@link RangePolicy} defining the number of stored events for
	 *         the topic. If no configuration is set for the topic filter then
	 *         an {@link RangePolicy#none()} will be returned
	 * @throws NullPointerException if the topic name is <code>null</code>
	 * @throws IllegalArgumentException if the topic name contains invalid
	 *             syntax or wildcards
	 * @since 1.1
	 */
	RangePolicy getEffectiveHistoryStorage(String topicName);

	/**
	 * Configure history storage for a given topic filter.
	 * <p>
	 * Minimum storage settings may only be set for exact matches. It is an
	 * error to use a filter containing wildcards with a non-zero minimum
	 * history requirement.
	 * <p>
	 * If a minimum storage requirement is set then the Typed Events
	 * implementation must guarantee sufficient storage space to hold those
	 * events. If, after accounting for all other pre-existing miniumum storage
	 * requirements, there is insufficent storage left for this new
	 * configuration then an {@link IllegalStateException} must be thrown.
	 * 
	 * @param topicFilter the topic filter
	 * @param policy the event retention policy to use
	 * @return An int indicating the number of events that can be kept for this
	 *         topic given the current configuration. This will always be at
	 *         least <code>policy.getMin()</code> and at most
	 *         <code>policy.getMax()</code>.
	 * @throws NullPointerException if the topic filter is <code>null</code>
	 * @throws IllegalArgumentException if:
	 *             <ul>
	 *             <li>The topic filter contains invalid syntax</li>
	 *             <li>The topic filter contains wildcard(s) <em>and</em>
	 *             <code>getMaximumEventStorage</code> is not <code>-1</code>
	 *             <em>and</em> <code>policy.getMin()</code> is not
	 *             <code>0</code>.</li>
	 *             </ul>
	 * @throws IllegalStateException if there is insufficient available space to
	 *             provide the additional <code>minRequired</code> stored
	 *             events.
	 * @since 1.1
	 */
	int configureHistoryStorage(String topicFilter, RangePolicy policy);

	/**
	 * Delete history storage configuration for a given topic filter.
	 * 
	 * @param topicFilter the topic filter
	 * @throws NullPointerException if the topic filter is <code>null</code>
	 * @throws IllegalArgumentException if the topic filter contains invalid
	 *             syntax
	 * @since 1.1
	 */
	void removeHistoryStorage(String topicFilter);

}
