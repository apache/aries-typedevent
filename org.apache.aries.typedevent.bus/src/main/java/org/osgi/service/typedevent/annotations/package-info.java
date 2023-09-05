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

/**
 * Typed Event Annotations Package Version 1.0.
 * <p>
 * This package contains annotations that can be used to require the Typed Event
 * implementation.
 * <p>
 * Bundles should not normally need to import this package as the annotations
 * are only used at build-time.
 * 
 * @author $Id: 627e9ee98bd7b640ebad077fccfbd6994c9af41e $
 */

@Export
@Version(TYPED_EVENT_SPECIFICATION_VERSION)
package org.osgi.service.typedevent.annotations;

import static org.osgi.service.typedevent.TypedEventConstants.TYPED_EVENT_SPECIFICATION_VERSION;

import org.osgi.annotation.bundle.Export;
import org.osgi.annotation.versioning.Version;

