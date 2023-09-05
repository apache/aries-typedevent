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
package org.apache.aries.typedevent.remote.remoteservices.osgi;

import static org.apache.aries.typedevent.remote.api.RemoteEventConstants.RECEIVE_REMOTE_EVENTS;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_FILTER;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_TOPICS;

import java.io.File;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.aries.typedevent.remote.remoteservices.common.TestEvent;
import org.apache.aries.typedevent.remote.remoteservices.spi.RemoteEventBus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceException;
import org.osgi.framework.ServiceFactory;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.osgi.framework.wiring.FrameworkWiring;
import org.osgi.service.typedevent.TypedEventBus;
import org.osgi.service.typedevent.UnhandledEventHandler;
import org.osgi.service.typedevent.UntypedEventHandler;
import org.osgi.test.common.annotation.InjectBundleContext;
import org.osgi.test.common.annotation.InjectService;
import org.osgi.test.junit5.context.BundleContextExtension;
import org.osgi.test.junit5.service.ServiceExtension;
import org.osgi.util.tracker.ServiceTracker;

/**
 * This is a JUnit test that will be run inside an OSGi framework.
 * 
 * It can interact with the framework by starting or stopping bundles, getting
 * or registering services, or in other ways, and then observing the result on
 * the bundle(s) being tested.
 */
@ExtendWith(BundleContextExtension.class)
@ExtendWith(ServiceExtension.class)
public class RemoteEventBusIntegrationTest extends AbstractIntegrationTest {

    private static final String REMOTE_BUS = RemoteEventBus.class.getName();
    private static final String UNTYPED_HANDLER = UntypedEventHandler.class.getName();
    private static final String UNHANDLED_HANDLER = UnhandledEventHandler.class.getName();
    private Map<UUID, Framework> frameworks;
    private Map<UUID, ServiceTracker<?,?>> remoteServicePublishers = new ConcurrentHashMap<>();
    
    @InjectBundleContext
    BundleContext bundleContext;
    
    @InjectService
    TypedEventBus bus;

    @Mock
    UntypedEventHandler untypedEventHandler, untypedEventHandler2;

    @Mock
    UnhandledEventHandler unhandledEventHandler;
    
    AutoCloseable mocks;
    
    @BeforeEach
    public void setUpFrameworks() throws Exception {
        mocks = MockitoAnnotations.openMocks(this);
        
        assertNotNull(bundleContext, "OSGi Bundle tests must be run inside an OSGi framework");

        frameworks = createFrameworks(2);
        frameworks.put(getMasterFrameworkUUID(), bundleContext.getBundle(0).adapt(Framework.class));
        
        for (Entry<UUID, Framework> entry : frameworks.entrySet()) {
            Framework f = entry.getValue();
            
            BundleContext context = f.getBundleContext();
            ServiceTracker<Object, Object> tracker = createCrossFrameworkPublisher(entry, context);
            
            remoteServicePublishers.put(entry.getKey(), tracker);
        }
    }

    private ServiceTracker<Object, Object> createCrossFrameworkPublisher(Entry<UUID, Framework> entry,
            BundleContext context) {
        ServiceTracker<Object, Object> tracker = new ServiceTracker<Object, Object>(context, 
                REMOTE_BUS, null) {
            
            Map<UUID, ServiceRegistration<?>> registered = new ConcurrentHashMap<>();

                    @Override
                    public Object addingService(ServiceReference<Object> reference) {
                        
                        if(reference.getBundle().getBundleId() == 0) {
                            return null;
                        }
                        
                        Object service = super.addingService(reference);

                        for (Entry<UUID, Framework> e : frameworks.entrySet()) {
                            UUID fwkId = entry.getKey();
                            if(fwkId.equals(e.getKey())) {
                                // Skip this framework as it's the same framework the service came from
                                continue;
                            }
                            
                            Framework fw = e.getValue();
                            
                            registered.put(fwkId, fw.getBundleContext().registerService(
                                    REMOTE_BUS, new EventHandlerFactory(service, REMOTE_BUS), 
                                    getRegistrationProps(reference)));
                        }
                        
                        return service;
                    }
                    
                    Dictionary<String, Object> getRegistrationProps(ServiceReference<?> ref) {
                        Dictionary<String, Object> toReturn = new Hashtable<String, Object>();
                        String[] props = ref.getPropertyKeys();
                        for(String key : props) {
                            toReturn.put(key, ref.getProperty(key));
                        }
                        
                        toReturn.put("service.imported", true);
                        return toReturn;
                    }

                    @Override
                    public void modifiedService(ServiceReference<Object> reference, Object service) {
                        for(ServiceRegistration<?> reg : registered.values()) {
                            reg.setProperties(getRegistrationProps(reference));
                        }
                    }

                    @Override
                    public void removedService(ServiceReference<Object> reference, Object service) {
                        for (ServiceRegistration<?> registration : registered.values()) {
                            try {
                                registration.unregister();
                            } catch (Exception e) {
                                // Never mind
                            }
                        }
                        registered.clear();
                        super.removedService(reference, service);
                    }
            
        };
        tracker.open(true);
        return tracker;
    }
    
    @AfterEach
    public void shutdownFrameworks() throws Exception {
        
        frameworks.remove(getMasterFrameworkUUID());
        
        remoteServicePublishers.values().forEach(ServiceTracker::close);
        remoteServicePublishers.clear();
        
        frameworks.values().forEach(f -> {
            try {
                f.stop();
            } catch (BundleException be) {
                // Never mind
            }
        });
        
        frameworks.clear();
        
        mocks.close();
    }

    private Map<UUID, Framework> createFrameworks(int size) throws BundleException {
        
        FrameworkFactory ff = ServiceLoader.load(FrameworkFactory.class, 
                FrameworkFactory.class.getClassLoader()).iterator().next();
        
        List<String> locations = new ArrayList<>();
        
        for(Bundle b : bundleContext.getBundles()) {
            if(
                    b.getSymbolicName().equals("org.apache.aries.typedevent.bus") ||
                    b.getSymbolicName().equals("org.apache.aries.typedevent.remote.api") ||
                    b.getSymbolicName().equals("org.apache.aries.typedevent.remote.spi") ||
                    b.getSymbolicName().equals("org.apache.aries.typedevent.remote.remoteservices") ||
                    b.getSymbolicName().equals("org.apache.aries.component-dsl.component-dsl") ||
                    b.getSymbolicName().equals("org.apache.felix.configadmin") ||
                    b.getSymbolicName().equals("org.osgi.service.typedevent") ||
                    b.getSymbolicName().equals("org.osgi.util.converter") ||
                    b.getSymbolicName().equals("org.osgi.util.function") ||
                    b.getSymbolicName().equals("org.osgi.util.promise") ||
                    b.getSymbolicName().equals("org.osgi.util.pushstream") ||
                    b.getSymbolicName().equals("slf4j.api") ||
                    b.getSymbolicName().startsWith("ch.qos.logback")) {
                locations.add(b.getLocation());
            }
        }
        
        Map<UUID, Framework> frameworks = new HashMap<UUID, Framework>();
        for(int i = 1; i < size; i++) {
            Map<String, String> fwConfig = new HashMap<>();
            fwConfig.put(Constants.FRAMEWORK_STORAGE, new File(bundleContext.getDataFile(""), "Test-Cluster" + i).getAbsolutePath());
            fwConfig.put(Constants.FRAMEWORK_STORAGE_CLEAN, Constants.FRAMEWORK_STORAGE_CLEAN_ONFIRSTINIT);
            Framework f = ff.newFramework(fwConfig);
            f.init();
            for(String s : locations) {
                f.getBundleContext().installBundle(s);
            }
            f.start();
            f.adapt(FrameworkWiring.class).resolveBundles(Collections.emptySet());
            for(Bundle b : f.getBundleContext().getBundles()) {
                if(b.getHeaders().get(Constants.FRAGMENT_HOST) == null) {
                    b.start();
                }
            }
            frameworks.put(getUUID(f), f);
        }
        return frameworks;
    }

    private UUID getMasterFrameworkUUID() {
        return UUID.fromString(bundleContext.getProperty(Constants.FRAMEWORK_UUID));
    }
    
    private UUID getUUID(Framework f) {
        return UUID.fromString(f.getBundleContext().getProperty(Constants.FRAMEWORK_UUID));
    }


    public static class EventHandlerFactory implements ServiceFactory<Object> {

        private final Object delegate;
        private final String typeToMimic;
        
        public EventHandlerFactory(Object delegate, String typeToMimic) {
            this.delegate = delegate;
            this.typeToMimic = typeToMimic;
        }

        @Override
        public Object getService(Bundle bundle, ServiceRegistration<Object> registration) {
            
            try {
                Class<?> loadClass = bundle.loadClass(typeToMimic);
                
                return Proxy.newProxyInstance(loadClass.getClassLoader(), new Class<?>[] {loadClass}, 
                        (o,m,a) -> {
                            
                            if(m.getName().startsWith("notify") && m.getParameterTypes().length > 0) {
                                return delegate.getClass().getMethod(m.getName(), m.getParameterTypes())
                                        .invoke(delegate, a);
                            } else {
                                return m.invoke(delegate, a);
                            }
                        });
                
            } catch (Exception e) {
                throw new ServiceException("failed to create service", e);
            }
        }

        @Override
        public void ungetService(Bundle bundle, ServiceRegistration<Object> registration, Object service) {
            // TODO Auto-generated method stub
            
        }
        
    }
    
    @Test
    public void testSendToRemoteFramework() throws InterruptedException {
        
        Dictionary<String, Object> props = new Hashtable<>();
        regs.add(bundleContext.registerService(UNHANDLED_HANDLER, unhandledEventHandler, props));
        
        TestEvent event = new TestEvent();
        event.message = "boo";
        
        bus.deliver(event);
        
        
        verify(unhandledEventHandler, Mockito.after(100).times(1))
            .notifyUnhandled(eq(TEST_EVENT_TOPIC), argThat(isUntypedTestEventWithMessage("boo")));
        
        
        BundleContext remoteContext = frameworks.values().stream()
                .filter(fw -> !getUUID(fw).equals(getMasterFrameworkUUID()))
                .flatMap(fw -> Arrays.stream(fw.getBundleContext().getBundles()))
                .filter(b -> b.getSymbolicName().equals("org.osgi.service.typedevent"))
                .map(Bundle::getBundleContext)
                .findFirst()
                .get();
        
        props = new Hashtable<>();
        props.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        props.put(RECEIVE_REMOTE_EVENTS, true);
        props.put(TYPED_EVENT_FILTER, "(message=boo)");
        
        regs.add(remoteContext.registerService(UNTYPED_HANDLER, 
                new EventHandlerFactory(untypedEventHandler, UNTYPED_HANDLER), props));

        props = new Hashtable<>();
        props.put(TYPED_EVENT_TOPICS, TEST_EVENT_TOPIC);
        props.put(RECEIVE_REMOTE_EVENTS, false);
        props.put(TYPED_EVENT_FILTER, "(message=far)");
        
        regs.add(remoteContext.registerService(UNTYPED_HANDLER, 
                new EventHandlerFactory(untypedEventHandler2, UNTYPED_HANDLER), props));
        
        
        bus.deliver(event);
        
        verify(unhandledEventHandler, Mockito.after(1000).times(1))
            .notifyUnhandled(eq(TEST_EVENT_TOPIC), argThat(isUntypedTestEventWithMessage("boo")));

        verify(untypedEventHandler2, Mockito.after(1000).never())
            .notifyUntyped(eq(TEST_EVENT_TOPIC), argThat(isUntypedTestEventWithMessage("boo")));
        
        verify(untypedEventHandler)
            .notifyUntyped(eq(TEST_EVENT_TOPIC), argThat(isUntypedTestEventWithMessage("boo")));
        
        event = new TestEvent();
        event.message = "far";
        
        bus.deliver(event);
        
        verify(unhandledEventHandler, Mockito.after(100).times(1))
        .notifyUnhandled(eq(TEST_EVENT_TOPIC), argThat(isUntypedTestEventWithMessage("far")));

        verify(untypedEventHandler2, Mockito.after(1000).never())
        .notifyUntyped(eq(TEST_EVENT_TOPIC), argThat(isUntypedTestEventWithMessage("far")));
    
        verify(untypedEventHandler, Mockito.after(1000).never())
        .notifyUntyped(eq(TEST_EVENT_TOPIC), argThat(isUntypedTestEventWithMessage("far")));
    }
    
}