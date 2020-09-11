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

import java.util.Map;

import org.osgi.service.typedevent.UnhandledEventHandler;

class UnhandledEventTask extends EventTask {
    private final String topic;
    private final EventConverter eventData;
    private final UnhandledEventHandler eventProcessor;

    public UnhandledEventTask(String topic, EventConverter eventData, UnhandledEventHandler eventProcessor) {
        super();
        this.topic = topic;
        this.eventData = eventData;
        this.eventProcessor = eventProcessor;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void notifyListener() {
        try {
            eventProcessor.notifyUnhandled(topic, (Map<String, Object>) eventData.toUntypedEvent());
        } catch (Exception e) {
            // TODO log this, also blacklist?
        }
    }
}