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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.aries.typedevent.bus.hidden.HiddenEvents;
import org.junit.jupiter.api.Test;
import org.osgi.util.converter.Converters;

/**
 * The flattened form of a DTO must be the one the converter produces, whether
 * the public fields of its class are cached or the converter is used
 */
public class EventConverterDtoFieldsTest {

    public static class Plain {
        public static String ignoredStatic = "static";
        public String name = "n";
        public int count = 3;
        public boolean flag = true;
        public List<String> tags = Arrays.asList("a", "b");
        public Plain nested;
    }

    public static class Mangled {
        public String a_b = "1";
        public String $c = "2";
        public String d__e = "3";
        public String f$_$g = "4";
    }

    public static class Prefixed {
        public static final String PREFIX_ = "pre.";
        public String x = "1";
    }

    public static class Parent {
        public String shared = "parent";
        public String parentOnly = "po";
    }

    public static class Child extends Parent {
        public String shared = "child";
        public String childOnly = "co";
    }

    public static class Leaky {
        public String name = "n";
    }

    private static Map<String, Object> flatten(Object event) {
        return EventConverter.forTypedEvent(event, null).toUntypedEvent();
    }

    private static Map<String, Object> byConverter(Object event) {
        return Converters.standardConverter().convert(event).sourceAsDTO().to(EventConverter.MAP_WITH_STRING_KEYS);
    }

    @Test
    public void testPlainClass() {
        Plain plain = new Plain();

        Map<String, Object> flat = flatten(plain);

        assertEquals(byConverter(plain), flat);
        assertEquals("n", flat.get("name"));
        assertEquals(3, flat.get("count"));
        assertEquals(Arrays.asList("a", "b"), flat.get("tags"));
        assertTrue(!flat.containsKey("ignoredStatic"));
        assertTrue(flat.containsKey("nested"));
        assertNull(flat.get("nested"));
    }

    @Test
    public void testNestedPlainClass() {
        Plain inner = new Plain();
        inner.name = "inner";
        Plain outer = new Plain();
        outer.nested = inner;

        Map<String, Object> flat = flatten(outer);

        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) flat.get("nested");
        assertEquals("inner", nested.get("name"));
    }

    @Test
    public void testSecondEventOfTheSameClass() {
        Plain first = new Plain();
        first.name = "first";
        Plain second = new Plain();
        second.name = "second";

        assertEquals("first", flatten(first).get("name"));
        assertEquals("second", flatten(second).get("name"));
    }

    @Test
    public void testMangledNames() {
        Mangled mangled = new Mangled();

        Map<String, Object> flat = flatten(mangled);

        assertEquals(byConverter(mangled), flat);
        assertEquals("1", flat.get("a.b"));
        assertEquals("2", flat.get("c"));
        assertEquals("3", flat.get("d_e"));
        assertEquals("4", flat.get("f-g"));
    }

    @Test
    public void testPrefix() {
        Prefixed prefixed = new Prefixed();

        Map<String, Object> flat = flatten(prefixed);

        assertEquals(byConverter(prefixed), flat);
        assertEquals("1", flat.get("pre.x"));
    }

    @Test
    public void testPublicFieldOfAClassThatIsNotAccessible() {
        Object hidden = HiddenEvents.newHiddenEvent();

        Map<String, Object> flat = flatten(hidden);

        // The converter cannot read the field, so it leaves it out and so must we
        assertEquals(byConverter(hidden), flat);
        assertTrue(flat.isEmpty());

        // And the same the second time, when the fields of the class are cached
        assertTrue(flatten(hidden).isEmpty());
    }

    @Test
    public void testInheritedFields() {
        Child child = new Child();

        Map<String, Object> flat = flatten(child);

        assertEquals(byConverter(child), flat);
        assertEquals("po", flat.get("parentOnly"));
        assertEquals("co", flat.get("childOnly"));
    }

    @Test
    public void testCachedFieldsDoNotKeepTheClassLoaderAlive() throws Exception {
        WeakReference<ClassLoader> loader = flattenInIsolatedLoader();

        long deadline = System.nanoTime() + 10_000_000_000L;
        while (loader.get() != null && System.nanoTime() < deadline) {
            System.gc();
            Thread.sleep(50);
        }

        assertNull(loader.get(), "The class loader of an event class was not released");
    }

    private WeakReference<ClassLoader> flattenInIsolatedLoader() throws Exception {
        IsolatedLoader loader = new IsolatedLoader(getClass().getClassLoader(), Leaky.class.getName());

        Object event = loader.loadClass(Leaky.class.getName()).getDeclaredConstructor().newInstance();
        assertEquals("n", flatten(event).get("name"));

        return new WeakReference<ClassLoader>(loader);
    }

    /**
     * Defines one class itself, so that it can be unloaded
     */
    private static class IsolatedLoader extends ClassLoader {

        private final String isolated;

        IsolatedLoader(ClassLoader parent, String isolated) {
            super(parent);
            this.isolated = isolated;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!isolated.equals(name)) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    try (InputStream in = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
                        byte[] bytes = in.readAllBytes();
                        loaded = defineClass(name, bytes, 0, bytes.length);
                    } catch (Exception e) {
                        throw new ClassNotFoundException(name, e);
                    }
                }
                return loaded;
            }
        }
    }
}
