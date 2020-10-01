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
import static java.util.function.Function.identity;
import static org.apache.aries.component.dsl.OSGi.all;
import static org.apache.aries.component.dsl.OSGi.bundleContext;
import static org.apache.aries.component.dsl.OSGi.coalesce;
import static org.apache.aries.component.dsl.OSGi.configuration;
import static org.apache.aries.component.dsl.OSGi.just;
import static org.apache.aries.component.dsl.OSGi.once;
import static org.apache.aries.component.dsl.OSGi.register;
import static org.apache.aries.component.dsl.OSGi.service;
import static org.apache.aries.component.dsl.OSGi.serviceReferences;
import static org.apache.aries.typedevent.remote.spi.LocalEventConsumerManager.ARIES_LOCAL_EVENT_PROXY;
import static org.osgi.framework.Constants.BUNDLE_ACTIVATOR;
import static org.osgi.framework.Constants.SERVICE_ID;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_TOPICS;
import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_FILTER;
import static org.osgi.util.converter.Converters.standardConverter;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.aries.component.dsl.OSGi;
import org.apache.aries.component.dsl.OSGiResult;
import org.apache.aries.typedevent.remote.api.RemoteEventMonitor;
import org.apache.aries.typedevent.remote.remoteservices.spi.RemoteEventBus;
import org.apache.aries.typedevent.remote.spi.RemoteEventMonitorImpl;
import org.osgi.annotation.bundle.Header;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.service.typedevent.TypedEventBus;
import org.osgi.service.typedevent.TypedEventConstants;
import org.osgi.service.typedevent.TypedEventHandler;
import org.osgi.service.typedevent.UntypedEventHandler;
import org.osgi.service.typedevent.monitor.TypedEventMonitor;
import org.osgi.util.converter.TypeReference;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Header(name = BUNDLE_ACTIVATOR, value = "${@class}")
public class RemoteServiceEventsActivator implements BundleActivator {

    private static final Logger _log = LoggerFactory.getLogger(RemoteServiceEventsActivator.class);

    OSGiResult eventBus;

    @Override
    public void start(BundleContext bundleContext) throws Exception {
        if (_log.isDebugEnabled()) {
            _log.debug("Aries Remote Typed Events (Remote Services) Starting");
        }

        eventBus = coalesce(configuration("org.apache.aries.typedevent.remote.remoteservices"), just(Hashtable::new))
                .map(this::toConfigProps).flatMap(configuration -> createProgram(configuration)).run(bundleContext);

        if (_log.isDebugEnabled()) {
            _log.debug("Aries Typed Event Bus Started");
        }
    }

    private OSGi<?> createProgram(Map<String, ?> configuration) {

        OSGi<Object> monitor = service(once(serviceReferences(TypedEventMonitor.class)))
                .map(RemoteEventMonitorImpl::new)
                .flatMap(remi -> register(RemoteEventMonitor.class, remi, new HashMap<>()));

        OSGi<Object> remote = bundleContext().flatMap(ctx -> service(once(serviceReferences(TypedEventBus.class)))
                .map(teb -> new RemoteEventBusImpl(teb, configuration))
                .effects(rebi -> rebi.init(ctx), rebi -> rebi.destroy())
                .flatMap(rebi -> all(
                        just(new UntypedEventTracker(ctx, rebi)).map(ServiceTracker.class::cast)
                                .effects(st -> st.open(), st -> st.close()),
                        just(new TypedEventTracker(ctx, rebi)).map(ServiceTracker.class::cast).effects(st -> st.open(),
                                st -> st.close()))));

        OSGi<Object> local = bundleContext()
                .flatMap(ctx -> just(new LocalEventBusForwarder()).effects(lebf -> lebf.start(ctx), lebf -> lebf.stop())
                        .flatMap(lebf -> serviceReferences(RemoteEventBus.class, "(service.imported=true)", csr -> {
                            lebf.updatedRemoteEventBus(getServiceProps(csr.getServiceReference()));
                            return false;
                        }).flatMap(csr -> service(csr).effects(
                                reb -> lebf.addRemoteEventBus(reb, getServiceProps(csr.getServiceReference())),
                                reb -> lebf.removeRemoteEventBus(reb, getServiceProps(csr.getServiceReference()))))));

        return all(monitor, remote, local);
    }

    private Map<String, Object> toConfigProps(Dictionary<String, ?> config) {
        Enumeration<String> keys = config.keys();
        Map<String, Object> map = new HashMap<>();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            map.put(key, config.get(key));
        }
        return map;
    }

    private static Map<String, Object> getServiceProps(ServiceReference<?> ref) {
        return Arrays.stream(ref.getPropertyKeys()).collect(Collectors.toMap(identity(), ref::getProperty));
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        if (_log.isDebugEnabled()) {
            _log.debug("Aries Typed Event Bus Stopping");
        }

        eventBus.close();

        if (_log.isDebugEnabled()) {
            _log.debug("Aries Typed Event Bus Stopped");
        }
    }

    private static final TypeReference<List<String>> LIST_OF_STRINGS = new TypeReference<List<String>>() {
    };

    private static Long getServiceId(ServiceReference<?> ref) {
        return standardConverter().convert(ref.getProperty(SERVICE_ID)).to(Long.class);
    }

    private static List<String> getTopics(ServiceReference<?> ref) {
        return standardConverter().convert(ref.getProperty(TYPED_EVENT_TOPICS)).to(LIST_OF_STRINGS);
    }

    private static Filter getFilter(ServiceReference<?> ref) throws InvalidSyntaxException {
        String filter = standardConverter().convert(ref.getProperty(TYPED_EVENT_FILTER)).to(String.class);
        if (filter == null || "".equals(filter)) {
            return null;
        } else {
            return FrameworkUtil.createFilter(filter);
        }
    }

    private static class UntypedEventTracker extends ServiceTracker<UntypedEventHandler, Object> {

        private final RemoteEventBusImpl impl;

        public UntypedEventTracker(BundleContext context, RemoteEventBusImpl impl) {
            super(context, UntypedEventHandler.class, null);
            this.impl = impl;
        }

        @Override
        public Object addingService(ServiceReference<UntypedEventHandler> reference) {
            
            if(TRUE.equals(reference.getProperty(ARIES_LOCAL_EVENT_PROXY))) {
                // Ignore remote interest proxies
                return null;
            }
            
            Filter filter;
            try {
                filter = getFilter(reference);
            } catch (InvalidSyntaxException e) {
                // TODO Auto-generated catch block
                return reference;
            }
            impl.updateLocalInterest(getServiceId(reference), getTopics(reference), filter, getServiceProps(reference));
            return reference;
        }

        @Override
        public void modifiedService(ServiceReference<UntypedEventHandler> reference, Object service) {
            Filter filter;
            try {
                filter = getFilter(reference);
            } catch (InvalidSyntaxException e) {
                // TODO Auto-generated catch block
                impl.removeLocalInterest(getServiceId(reference));
                return;
            }
            impl.updateLocalInterest(getServiceId(reference), getTopics(reference), filter, getServiceProps(reference));
        }

        @Override
        public void removedService(ServiceReference<UntypedEventHandler> reference, Object service) {
            impl.removeLocalInterest(getServiceId(reference));
        }
    };

    @SuppressWarnings("rawtypes")
    private static class TypedEventTracker extends ServiceTracker<TypedEventHandler, TypedEventHandler> {

        private final RemoteEventBusImpl impl;

        public TypedEventTracker(BundleContext context, RemoteEventBusImpl impl) {
            super(context, TypedEventHandler.class, null);
            this.impl = impl;
        }

        @Override
        public TypedEventHandler addingService(ServiceReference<TypedEventHandler> reference) {
            TypedEventHandler toReturn = context.getService(reference);
            Filter filter;
            try {
                filter = getFilter(reference);
            } catch (InvalidSyntaxException e) {
                // TODO Auto-generated catch block
                return toReturn;
            }
            List<String> topics = findTopics(reference, toReturn);
            if (!topics.isEmpty()) {
                impl.updateLocalInterest(getServiceId(reference), topics, filter, getServiceProps(reference));
            }
            return toReturn;
        }

        private List<String> findTopics(ServiceReference<TypedEventHandler> reference, TypedEventHandler service) {
            List<String> topics = getTopics(reference);
            if (topics.isEmpty()) {
                Object type = reference.getProperty(TypedEventConstants.TYPED_EVENT_TYPE);
                if (type != null) {
                    topics = Collections.singletonList(String.valueOf(type).replace(".", "/"));
                } else {
                    Class<?> clazz = discoverTypeForTypedHandler(service);
                    if (clazz != null) {
                        topics = Collections.singletonList(clazz.getName().replace(".", "/"));
                    }
                }
            }
            return topics;
        }

        /**
         * Extensively copied from the Core Event Bus - is there a better way to share
         * this?
         * 
         * @param handler
         * @param properties
         * @return
         */
        private Class<?> discoverTypeForTypedHandler(TypedEventHandler<?> handler) {
            Class<?> clazz = null;
            Class<?> toCheck = handler.getClass();
            while (clazz == null) {
                clazz = findDirectlyImplemented(toCheck);

                if (clazz != null) {
                    break;
                }

                clazz = processInterfaceHierarchyForClass(toCheck);

                if (clazz != null) {
                    break;
                }

                toCheck = toCheck.getSuperclass();
            }

            return clazz;
        }

        private Class<?> processInterfaceHierarchyForClass(Class<?> toCheck) {
            Class<?> clazz = null;
            for (Class<?> iface : toCheck.getInterfaces()) {
                clazz = findDirectlyImplemented(iface);

                if (clazz != null) {
                    break;
                }

                clazz = processInterfaceHierarchyForClass(iface);

                if (clazz != null) {
                    break;
                }
            }
            return clazz;
        }

        private Class<?> findDirectlyImplemented(Class<?> toCheck) {
            return Arrays.stream(toCheck.getGenericInterfaces()).filter(ParameterizedType.class::isInstance)
                    .map(ParameterizedType.class::cast).filter(t -> TypedEventHandler.class.equals(t.getRawType()))
                    .map(t -> t.getActualTypeArguments()[0]).findFirst().map(Class.class::cast).orElse(null);
        }

        @Override
        public void modifiedService(ServiceReference<TypedEventHandler> reference, TypedEventHandler service) {
            Filter filter;
            try {
                filter = getFilter(reference);
            } catch (InvalidSyntaxException e) {
                // TODO Auto-generated catch block
                impl.removeLocalInterest(getServiceId(reference));
                return;
            }

            List<String> topics = findTopics(reference, service);
            if (topics.isEmpty()) {
                impl.removeLocalInterest(getServiceId(reference));
            } else {
                impl.updateLocalInterest(getServiceId(reference), getTopics(reference), filter, getServiceProps(reference));
            }
        }

        @Override
        public void removedService(ServiceReference<TypedEventHandler> reference, TypedEventHandler service) {
            impl.removeLocalInterest(getServiceId(reference));
        }
    };
}
