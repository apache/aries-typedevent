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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class EventTask {
	
	private static final Logger _log = LoggerFactory.getLogger(EventTask.class);
	
    public void notifyListener() {
    	try {
            unsafeNotify();
        } catch (Exception e) {
        	// TODO blacklisting?
            _log.error("The event delivery failed for task type.", getClass(), e);
        }
    }
    
    protected abstract void unsafeNotify() throws Exception;
    
}