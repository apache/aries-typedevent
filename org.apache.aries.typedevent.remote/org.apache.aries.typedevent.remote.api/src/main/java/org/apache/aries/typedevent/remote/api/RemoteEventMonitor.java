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
package org.apache.aries.typedevent.remote.api;

import org.osgi.annotation.versioning.ProviderType;
import org.osgi.util.pushstream.PushStream;

import java.time.Instant;

/**
 * The {@link RemoteEventMonitor} service can be used to monitor the events that are
 * sent using the EventBus, and that are received from remote EventBus
 * instances
 */
@ProviderType
public interface RemoteEventMonitor {

    /**
     * Get a stream of events that match any of the filters, starting now.
     * <p>
     * Filter expressions may be supplied and applied by the monitoring implementation.
     * In some cases this may be more optimal than adding your own filter to the returned
     * PushStream.
     *
     * @param filters containing filter expression definitions. The {@link RemoteMonitorEvent#publishType} 
     *                field is available with the key <code>-publishType</code>, in addition to fields 
     *                defined in the event.
     *                <p>
     *                If the event contains nested data structures then those are accessible using
     *                nested key names separated by a '.' character (e.g. <code>"foo.bar"</code>
     *                would correspond to the <code>bar</code> field of the <code>foo</code> value
     *                from the event.
     *                <p>
     *                If a {@link FilterDTO} contains both LDAP and regular expressions, then both must match.
     *                A RegEx pattern allows the whole event content to be matched, without necessarily specifying
     *                a key (although keys are present and separated with ':').
     * @return A stream of event data
     */
    PushStream<RemoteMonitorEvent> monitorEvents(FilterDTO... filters);

    /**
     * Get a stream of events, including up to the
     * requested number of historical data events, that match any of the filters.
     *
     * @param history The requested number of historical
     * events, note that fewer than this number of events
     * may be returned if history is unavailable, or if
     * insufficient events have been sent.
     *
     * @param filters containing filter expression definitions. The {@link RemoteMonitorEvent#publishType} 
     *                field is available with the key <code>-publishType</code>, in addition to fields 
     *                defined in the event.
     *                <p>
     *                If the event contains nested data structures then those are accessible using
     *                nested key names separated by a '.' character (e.g. <code>"foo.bar"</code>
     *                would correspond to the <code>bar</code> field of the <code>foo</code> value
     *                from the event.
     *                <p>
     *                If a {@link FilterDTO} contains both LDAP and regular expressions, then both must match.
     *                A RegEx pattern allows the whole event content to be matched, without necessarily specifying
     *                a key (although keys are present and separated with ':').
     *
     * @return A stream of event data
     */
    PushStream<RemoteMonitorEvent> monitorEvents(int history, FilterDTO...filters);

    /**
     * Get a stream of events, including historical
     * data events prior to the supplied time
     *
     * @param history The requested time after which
     * historical events, should be included. Note
     * that events may have been discarded, or history
     * unavailable.
     *
     * @param filters containing filter expression definitions. The {@link RemoteMonitorEvent#publishType} 
     *                field is available with the key <code>-publishType</code>, in addition to fields 
     *                defined in the event.
     *                <p>
     *                If the event contains nested data structures then those are accessible using
     *                nested key names separated by a '.' character (e.g. <code>"foo.bar"</code>
     *                would correspond to the <code>bar</code> field of the <code>foo</code> value
     *                from the event.
     *                <p>
     *                If a {@link FilterDTO} contains both LDAP and regular expressions, then both must match.
     *                A RegEx pattern allows the whole event content to be matched, without necessarily specifying
     *                a key (although keys are present and separated with ':').
     *
     * @return A stream of event data
     */
    PushStream<RemoteMonitorEvent> monitorEvents(Instant history, FilterDTO...filters);

}
