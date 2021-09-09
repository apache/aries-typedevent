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
package org.apache.aries.typedevent.remote.remoteservices.spi;

import java.util.Map;

import org.osgi.annotation.versioning.ProviderType;

/**
 * This interface should not be used by typical users of the
 * Typed Event specification. It is intended to be a bridge
 * between different mechanisms for broadcasting remote events
 */

@ProviderType
public interface RemoteEventBus {
    
    /**
     * This service property provides a String+ containing &lt;topic&gt;=&lt;filter&gt; 
     * entries indicating the events that the remote nodes are interested in.
     */
    public static final String REMOTE_EVENT_FILTERS = "remote.event.filters";
    
    /**   
     * Called to notify this instance of an event from a remote framework
     * @param topic The topic
     * @param eventData The untyped event data
     */
    public void notify(String topic, Map<String, Object> eventData);
}
