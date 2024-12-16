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
import static org.apache.aries.component.dsl.OSGi.all;
import static org.apache.aries.component.dsl.OSGi.coalesce;
import static org.apache.aries.component.dsl.OSGi.configuration;
import static org.apache.aries.component.dsl.OSGi.nothing;
import static org.apache.aries.component.dsl.OSGi.just;
import static org.apache.aries.component.dsl.OSGi.register;
import static org.apache.aries.component.dsl.OSGi.service;
import static org.apache.aries.component.dsl.OSGi.serviceReferences;

import java.util.Arrays;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.aries.component.dsl.OSGi;
import org.apache.aries.component.dsl.OSGiResult;
import org.apache.aries.typedevent.bus.spi.AriesTypedEvents;
import org.osgi.annotation.bundle.Header;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.service.typedevent.TypedEventBus;
import org.osgi.service.typedevent.TypedEventHandler;
import org.osgi.service.typedevent.UnhandledEventHandler;
import org.osgi.service.typedevent.UntypedEventHandler;
import org.osgi.service.typedevent.monitor.TypedEventMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Header(name = Constants.BUNDLE_ACTIVATOR, value = "${@class}")
public class TypedEventBusActivator implements BundleActivator {

    private static final Logger _log = LoggerFactory.getLogger(TypedEventBusActivator.class);

    OSGiResult eventBus;

    @Override
    public void start(BundleContext bundleContext) throws Exception {
        if (_log.isInfoEnabled()) {
            _log.info("Aries Typed Event Bus Starting");
        }

        eventBus = coalesce(
                    configuration("org.apache.aries.typedevent.bus"),
                    just(Hashtable::new)
                )
                .map(this::toConfigProps)
                .flatMap(configuration -> createProgram(configuration))
                .run(bundleContext);

        if (_log.isInfoEnabled()) {
            _log.info("Aries Typed Event Bus Started");
        }
    }

    private OSGi<?> createProgram(Map<String, ?> configuration) {

        Map<String, Object> serviceProps = toServiceProps(configuration);

        return just(configuration)
                .map(TypedEventMonitorImpl::new)
                .effects(x -> { }, TypedEventMonitorImpl::destroy)
                .flatMap(
                        temi -> register(TypedEventMonitor.class, temi, serviceProps)
                                .then(just(new TypedEventBusImpl(temi, configuration))
                                .effects(TypedEventBusImpl::start, TypedEventBusImpl::stop)))
                .flatMap(
                        tebi -> all(
                                serviceReferences(TypedEventHandler.class, 
                                        csr -> {
                                            tebi.updatedTypedEventHandler(
                                            		csr.getServiceReference().getBundle(),
                                                    getServiceProps(csr.getServiceReference()));
                                            return false;
                                        })
                                        .flatMap(csr -> service(csr)
                                                .effects(
                                                         handler -> tebi.addTypedEventHandler(
                                                        		 	csr.getServiceReference().getBundle(),
                                                        		 	handler,
                                                                    getServiceProps(csr.getServiceReference())),
                                                         handler -> tebi.removeTypedEventHandler(handler,
                                                                    getServiceProps(csr.getServiceReference())))),
                                serviceReferences(UntypedEventHandler.class, 
                                        csr -> {
                                            tebi.updatedUntypedEventHandler(
                                                    getServiceProps(csr.getServiceReference()));
                                            return false;
                                        })
                                        .flatMap(csr -> service(csr)
                                                .effects(
                                                         handler -> tebi.addUntypedEventHandler(handler,
                                                                    getServiceProps(csr.getServiceReference())),
                                                         handler -> tebi.removeUntypedEventHandler(handler,
                                                                    getServiceProps(csr.getServiceReference())))),
                                serviceReferences(UnhandledEventHandler.class)
                                        .flatMap(csr -> service(csr)
                                                .effects(handler -> tebi.addUnhandledEventHandler(handler,
                                                                    getServiceProps(csr.getServiceReference())),
                                                         handler -> tebi.removeUnhandledEventHandler(handler,
                                                                    getServiceProps(csr.getServiceReference())))),
                                register(new String[] { 
                        				TypedEventBus.class.getName(), AriesTypedEvents.class.getName()
                        			}, tebi, serviceProps)
                                        .flatMap(x -> nothing())));


    }

    private Map<String, Object> toConfigProps(Dictionary<String, ?> config) {
        Enumeration<String> keys = config.keys();
        Map<String, Object> map = new HashMap<>();
         while(keys.hasMoreElements()) {
            String key = keys.nextElement();
            map.put(key, config.get(key));
        }
        return map;
    }
    
    private Map<String, Object> toServiceProps(Map<String, ?> config) {
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
        if (_log.isInfoEnabled()) {
            _log.info("Aries Typed Event Bus Stopping");
        }

        eventBus.close();

        if (_log.isInfoEnabled()) {
            _log.info("Aries Typed Event Bus Stopped");
        }
    }
}