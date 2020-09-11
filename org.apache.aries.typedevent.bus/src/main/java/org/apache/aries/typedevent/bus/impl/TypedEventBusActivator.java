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

import static java.util.function.Function.identity;

import java.util.Arrays;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.osgi.annotation.bundle.Header;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.typedevent.TypedEventBus;
import org.osgi.service.typedevent.TypedEventHandler;
import org.osgi.service.typedevent.UnhandledEventHandler;
import org.osgi.service.typedevent.UntypedEventHandler;
import org.osgi.service.typedevent.monitor.TypedEventMonitor;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Header(name = Constants.BUNDLE_ACTIVATOR, value = "${@class}")
public class TypedEventBusActivator implements BundleActivator {

    private static final Logger _log = LoggerFactory.getLogger(TypedEventBusActivator.class);

    private TypedEventMonitorImpl monitorImpl;
    private ServiceRegistration<TypedEventMonitor> monitorReg;

    private TypedEventBusImpl busImpl;
    private ServiceRegistration<TypedEventBus> busReg;

    private ServiceTracker<TypedEventHandler<?>, TypedEventHandler<?>> typedTracker;
    private ServiceTracker<UntypedEventHandler, UntypedEventHandler> untypedTracker;
    private ServiceTracker<UnhandledEventHandler, UnhandledEventHandler> unhandledTracker;

    @Override
    public void start(BundleContext bundleContext) throws Exception {
        if (_log.isDebugEnabled()) {
            _log.debug("Aries Typed Event Bus Starting");
        }

        // TODO use Config Admin

        Map<String, Object> map = new HashMap<String, Object>();

        createEventBus(bundleContext, map);

        if (_log.isDebugEnabled()) {
            _log.debug("Aries Typed Event Bus Started");
        }
    }

    private void createEventBus(BundleContext bundleContext, Map<String, ?> configuration) throws Exception {

        Dictionary<String, Object> serviceProps = toServiceProps(configuration);

        monitorImpl = new TypedEventMonitorImpl(configuration);
        busImpl = new TypedEventBusImpl(monitorImpl, configuration);

        untypedTracker = new ServiceTracker<>(bundleContext, UntypedEventHandler.class,
                new ServiceTrackerCustomizer<UntypedEventHandler, UntypedEventHandler>() {

                    @Override
                    public UntypedEventHandler addingService(ServiceReference<UntypedEventHandler> reference) {
                        UntypedEventHandler service = bundleContext.getService(reference);
                        busImpl.addUntypedEventHandler(service, getServiceProps(reference));
                        return service;
                    }

                    @Override
                    public void modifiedService(ServiceReference<UntypedEventHandler> reference,
                            UntypedEventHandler service) {
                        busImpl.updatedUntypedEventHandler(service, getServiceProps(reference));
                    }

                    @Override
                    public void removedService(ServiceReference<UntypedEventHandler> reference,
                            UntypedEventHandler service) {
                        busImpl.removeUntypedEventHandler(service, getServiceProps(reference));
                    }
                });

        untypedTracker = new ServiceTracker<>(bundleContext, UntypedEventHandler.class,
                new ServiceTrackerCustomizer<UntypedEventHandler, UntypedEventHandler>() {

                    @Override
                    public UntypedEventHandler addingService(ServiceReference<UntypedEventHandler> reference) {
                        UntypedEventHandler service = bundleContext.getService(reference);
                        busImpl.addUntypedEventHandler(service, getServiceProps(reference));
                        return service;
                    }

                    @Override
                    public void modifiedService(ServiceReference<UntypedEventHandler> reference,
                            UntypedEventHandler service) {
                        busImpl.updatedUntypedEventHandler(service, getServiceProps(reference));
                    }

                    @Override
                    public void removedService(ServiceReference<UntypedEventHandler> reference,
                            UntypedEventHandler service) {
                        busImpl.removeUntypedEventHandler(service, getServiceProps(reference));
                    }
                });

        unhandledTracker = new ServiceTracker<>(bundleContext, UnhandledEventHandler.class,
                new ServiceTrackerCustomizer<UnhandledEventHandler, UnhandledEventHandler>() {

                    @Override
                    public UnhandledEventHandler addingService(ServiceReference<UnhandledEventHandler> reference) {
                        UnhandledEventHandler service = bundleContext.getService(reference);
                        busImpl.addUnhandledEventHandler(service, getServiceProps(reference));
                        return service;
                    }

                    @Override
                    public void modifiedService(ServiceReference<UnhandledEventHandler> reference,
                            UnhandledEventHandler service) {
                    }

                    @Override
                    public void removedService(ServiceReference<UnhandledEventHandler> reference,
                            UnhandledEventHandler service) {
                        busImpl.removeUnhandledEventHandler(service, getServiceProps(reference));
                    }
                });

        try {
            busImpl.start();

            monitorReg = bundleContext.registerService(TypedEventMonitor.class, monitorImpl, serviceProps);

            typedTracker.open();
            untypedTracker.open();
            unhandledTracker.open();

            busReg = bundleContext.registerService(TypedEventBus.class, busImpl, serviceProps);

        } catch (Exception e) {
            stop(bundleContext);
        }

    }

    private void safeUnregister(ServiceRegistration<?> reg) {
        try {
            reg.unregister();
        } catch (IllegalStateException ise) {
            // no op
            // TODO LOG this
        }

    }

    private Dictionary<String, Object> toServiceProps(Map<String, ?> config) {
        return config.entrySet().stream().filter(e -> e.getKey() != null && e.getKey().startsWith("."))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (a, b) -> {
                    throw new IllegalArgumentException("Duplicate key ");
                }, Hashtable::new));
    }

    private Map<String, Object> getServiceProps(ServiceReference<?> ref) {
        return Arrays.stream(ref.getPropertyKeys()).collect(Collectors.toMap(identity(), ref::getProperty));
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        if (_log.isDebugEnabled()) {
            _log.debug("Aries Typed Event Bus Stopping");
        }

        // Order matters here
        if (busReg != null) {
            safeUnregister(busReg);
        }

        if (busImpl != null) {
            busImpl.stop();
        }

        if (typedTracker != null) {
            typedTracker.close();
        }
        if (untypedTracker != null) {
            untypedTracker.close();
        }
        if (unhandledTracker != null) {
            unhandledTracker.close();
        }

        if (monitorReg != null) {
            safeUnregister(monitorReg);
        }

        monitorImpl.destroy();

        if (_log.isDebugEnabled()) {
            _log.debug("Aries Typed Event Bus Stopped");
        }
    }
}