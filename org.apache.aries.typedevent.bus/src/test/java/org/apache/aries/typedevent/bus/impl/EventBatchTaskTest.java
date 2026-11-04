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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

public class EventBatchTaskTest {

    private final List<String> ran = new ArrayList<>();

    private EventTask recording(String name) {
        return new EventTask() {
            @Override
            protected void unsafeNotify() {
                ran.add(name);
            }
        };
    }

    private EventTask failing(String name, Throwable failure) {
        return new EventTask() {
            @Override
            protected void unsafeNotify() throws Exception {
                ran.add(name);
                if (failure instanceof Exception) {
                    throw (Exception) failure;
                }
                throw (Error) failure;
            }
        };
    }

    @Test
    public void testTasksRunInOrderWithTheFirstTaskFirst() {
        new EventBatchTask(recording("first"), Arrays.asList(recording("a"), recording("b"), recording("c")))
                .notifyListener();

        assertEquals(Arrays.asList("first", "a", "b", "c"), ran);
    }

    @Test
    public void testNoOtherTasks() {
        new EventBatchTask(recording("first"), Collections.emptyList()).notifyListener();

        assertEquals(Arrays.asList("first"), ran);
    }

    @Test
    public void testFailingTasksDoNotStopTheOthers() {
        new EventBatchTask(failing("first", new IllegalStateException()), Arrays.asList(recording("a"),
                failing("b", new IllegalArgumentException()), failing("c", new StackOverflowError()), recording("d")))
                .notifyListener();

        assertEquals(Arrays.asList("first", "a", "b", "c", "d"), ran);
    }
}
