/*******************************************************************************
 * Copyright (c) Contributors to the Eclipse Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0 
 *******************************************************************************/

package org.osgi.service.typedevent.propertytypes;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.osgi.service.component.annotations.ComponentPropertyType;
import org.osgi.service.typedevent.TypedEventConstants;
import org.osgi.service.typedevent.TypedEventHandler;
import org.osgi.service.typedevent.annotations.RequireTypedEvent;

/**
 * Component Property Type for the {@link TypedEventConstants#TYPED_EVENT_TYPE}
 * service property of an {@link TypedEventHandler} service.
 * <p>
 * This annotation can be used on an {@link TypedEventHandler} component to
 * declare the value of the {@link TypedEventConstants#TYPED_EVENT_TYPE} service
 * property.
 * 
 * @see "Component Property Types"
 * @author $Id: 3799448fcf60ec8a125bf857b2b7da23d9f45570 $
 */
@ComponentPropertyType
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.TYPE)
@RequireTypedEvent
public @interface EventType {
	/**
	 * Service property specifying the {@code EventType} for a
	 * {@link TypedEventHandler} service.
	 * 
	 * @return The event filter.
	 * @see TypedEventConstants#TYPED_EVENT_TYPE
	 */
	Class<?> value();
}
