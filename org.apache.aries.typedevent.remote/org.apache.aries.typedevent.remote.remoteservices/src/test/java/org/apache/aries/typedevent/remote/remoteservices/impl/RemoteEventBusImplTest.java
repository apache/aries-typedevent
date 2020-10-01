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
package org.apache.aries.typedevent.remote.remoteservices.impl;

import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.aries.typedevent.remote.api.RemoteEventConstants.RECEIVE_REMOTE_EVENTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.osgi.framework.FrameworkUtil.createFilter;

import java.util.Arrays;
import java.util.Dictionary;
import java.util.HashMap;

import org.apache.aries.typedevent.remote.remoteservices.spi.RemoteEventBus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.typedevent.TypedEventBus;

@SuppressWarnings("unchecked")
public class RemoteEventBusImplTest {
    
    @Mock
    BundleContext context;
    
    @Mock
    ServiceRegistration<RemoteEventBus> remoteReg;
    @Mock
    ServiceReference<RemoteEventBus> remoteRef;
    
    @Mock
    TypedEventBus eventBusImpl;
    
    RemoteEventBusImpl remoteImpl;
    
    private AutoCloseable mocks;
    
    @BeforeEach
    public void start() {
        
        mocks = MockitoAnnotations.openMocks(this);
        
        Mockito.when(context.registerService(Mockito.eq(RemoteEventBus.class), 
                Mockito.any(RemoteEventBus.class), Mockito.any())).thenReturn(remoteReg);
        Mockito.when(remoteReg.getReference()).thenReturn(remoteRef);
        
        remoteImpl = new RemoteEventBusImpl(eventBusImpl, new HashMap<>());
    }

    
    @AfterEach
    public void destroy() throws Exception {
        remoteImpl.destroy();
        mocks.close();
    }
    
    @Test
    public void testEmptyStart() {
        remoteImpl.init(context);
        
        ArgumentCaptor<Dictionary<String, Object>> propsCaptor = ArgumentCaptor.forClass(Dictionary.class); 
        
        Mockito.verify(context).registerService(Mockito.eq(RemoteEventBus.class), Mockito.same(remoteImpl),
                propsCaptor.capture());
        
        Dictionary<String, Object> props = propsCaptor.getValue();
        assertNull(props);
        
        Mockito.verify(remoteReg).setProperties(propsCaptor.capture());
        
        props = propsCaptor.getValue();
        
        assertEquals(RemoteEventBus.class.getName(), props.get("service.exported.interfaces"));
        assertEquals(emptyList(), props.get(RemoteEventBus.REMOTE_EVENT_FILTERS));
    }

    @Test
    public void testStartWithDetails() throws InvalidSyntaxException {
        
        remoteImpl.updateLocalInterest(42L, Arrays.asList("FOO"), createFilter("(fizz=buzz)"), 
                singletonMap(RECEIVE_REMOTE_EVENTS, TRUE));
        
        remoteImpl.init(context);
        
        ArgumentCaptor<Dictionary<String, Object>> propsCaptor = ArgumentCaptor.forClass(Dictionary.class); 
        
        Mockito.verify(context).registerService(Mockito.eq(RemoteEventBus.class), Mockito.same(remoteImpl),
                propsCaptor.capture());
    
        Dictionary<String, Object> props = propsCaptor.getValue();
        assertNull(props);

        Mockito.verify(remoteReg).setProperties(propsCaptor.capture());
        
        props = propsCaptor.getValue();
        
        assertEquals(RemoteEventBus.class.getName(), props.get("service.exported.interfaces"));
        assertEquals(Arrays.asList("FOO=(fizz=buzz)"), props.get(RemoteEventBus.REMOTE_EVENT_FILTERS));
    }
    
    @Test
    public void testLateRegisterOfListener() throws InvalidSyntaxException {
        remoteImpl.init(context);
        
        ArgumentCaptor<Dictionary<String, Object>> propsCaptor = ArgumentCaptor.forClass(Dictionary.class); 
        
        Mockito.verify(context).registerService(Mockito.eq(RemoteEventBus.class), Mockito.same(remoteImpl),
                propsCaptor.capture());
        
        Dictionary<String, Object> props = propsCaptor.getValue();
        assertNull(props);
        
        Mockito.verify(remoteReg).setProperties(propsCaptor.capture());
        
        props = propsCaptor.getValue();
        
        assertEquals(RemoteEventBus.class.getName(), props.get("service.exported.interfaces"));
        assertEquals(emptyList(), props.get(RemoteEventBus.REMOTE_EVENT_FILTERS));
        
        // Add a listener to the remote
        
        remoteImpl.updateLocalInterest(42L, Arrays.asList("FOO"), createFilter("(fizz=buzz)"),
                singletonMap(RECEIVE_REMOTE_EVENTS, TRUE));
        
        Mockito.verify(remoteReg, Mockito.times(2)).setProperties(propsCaptor.capture());
        
        props = propsCaptor.getValue();
        
        assertEquals(RemoteEventBus.class.getName(), props.get("service.exported.interfaces"));
        assertEquals(Arrays.asList("FOO=(fizz=buzz)"), props.get(RemoteEventBus.REMOTE_EVENT_FILTERS));
    }
    
    @Test
    public void testStartWithNonRemoteListener() throws InvalidSyntaxException {
        
        remoteImpl.updateLocalInterest(42L, Arrays.asList("FOO"), createFilter("(fizz=buzz)"), 
                emptyMap());
        
        remoteImpl.init(context);
        
        ArgumentCaptor<Dictionary<String, Object>> propsCaptor = ArgumentCaptor.forClass(Dictionary.class); 
        
        Mockito.verify(context).registerService(Mockito.eq(RemoteEventBus.class), Mockito.same(remoteImpl),
                propsCaptor.capture());
    
        Dictionary<String, Object> props = propsCaptor.getValue();
        assertNull(props);

        Mockito.verify(remoteReg).setProperties(propsCaptor.capture());
        
        props = propsCaptor.getValue();
        
        assertEquals(RemoteEventBus.class.getName(), props.get("service.exported.interfaces"));
        assertEquals(emptyList(), props.get(RemoteEventBus.REMOTE_EVENT_FILTERS));
    }
}
