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

/**
 * This interface should not be used by typical users of the
 * Typed Event specification. It is intended to be a bridge
 * between different mechanisms for broadcasting remote events
 */

@ProviderType
public class RemoteEventConstants {
    
    /**
     * This property key will be set to true in any event that originated from a remote system.
     * This is to allow different remoting implementations to identify events which should not
     * be sent on externally, as they are already external.
     */
    public static final String REMOTE_EVENT_MARKER = ".org.apache.aries.typedevent.remote";
    
    /**
     * This service property can be used by Event Handler whiteboard services to signal that
     * they wish to receive remote events by using the value <code>true</code>. Depending 
     * upon the configuration of the remote event backend it may not be necessary to supply 
     * this property to receive remote events.
     */
    public static final String RECEIVE_REMOTE_EVENTS = "org.apache.aries.typedevent.remote.events";

    private RemoteEventConstants() {
        // Deliberately impossible to construct
    }
    
}
