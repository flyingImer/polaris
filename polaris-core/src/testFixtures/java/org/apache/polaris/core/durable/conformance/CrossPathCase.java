/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.core.durable.conformance;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;

/**
 * One record of a kind declaring more than one lookup path, with the anchor tuple under which that
 * single record must be served on EVERY declared path of its kind. The cross-path counterpart of
 * {@link PathCase}: a PathCase proves each path filters its own scope over records minted for that
 * path alone, so one record indexed under several paths is asserted by neither; this case commits
 * one record and reads it back through all of its kind's paths, and asserts it absent under another
 * path's anchors, so two paths accidentally wired to the same column cannot pass as distinct
 * directions.
 *
 * @param kind the record kind
 * @param record the one record, its field values chosen so every declared path of the kind serves
 *     it
 * @param anchorsPerPath for each declared path of the kind, the anchor tuple under which the record
 *     must appear
 * @param identityRef the record's identity reference, for containment assertions
 */
public record CrossPathCase(
    RecordKind kind,
    Object record,
    Map<LookupPath, List<Object>> anchorsPerPath,
    Function<Object, RecordRef> identityRef) {}
