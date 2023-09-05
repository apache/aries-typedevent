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

package org.osgi.service.typedevent.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.osgi.annotation.bundle.Requirement;
import org.osgi.namespace.implementation.ImplementationNamespace;
import org.osgi.service.typedevent.TypedEventConstants;

/**
 * This annotation can be used to require the Typed Event implementation. It can
 * be used directly, or as a meta-annotation.
 * <p>
 * This annotation is applied to several of the Typed Event component property
 * type annotations meaning that it does not normally need to be applied to
 * Declarative Services components which use the Typed Event specification.
 * 
 * @author $Id: 5919b947393ff3b07a0e8cd1dc881dcf4637e6b4 $
 * @since 1.0
 */
@Documented
@Retention(RetentionPolicy.CLASS)
@Target({
		ElementType.TYPE, ElementType.PACKAGE
})
@Requirement(namespace = ImplementationNamespace.IMPLEMENTATION_NAMESPACE, //
		name = TypedEventConstants.TYPED_EVENT_IMPLEMENTATION, //
		version = TypedEventConstants.TYPED_EVENT_SPECIFICATION_VERSION)
public @interface RequireTypedEvent {
	// This is a marker annotation.
}
