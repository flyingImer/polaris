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
package org.apache.polaris.spi.durable;

import org.jspecify.annotations.NonNull;

/**
 * A record together with the {@link ReadToken} the store attached when it read it.
 *
 * <p>{@link DurableRecordStore#read} returns this, and it is the only way a caller comes to hold a
 * token. A record kind carrying no version attribute cannot say "still the row I read" through
 * {@link Precondition#versionEquals}, so it reads through here and rides the token into its commit
 * as {@link Precondition#unchangedSince}.
 *
 * <p>Support data rather than a seam: it carries a value and declares no behaviour.
 *
 * @param value the record, exactly as {@link DurableRecordStore#get} would have returned it
 * @param token the issuing store's opaque marker for the state {@code value} was read in
 */
public record Read<T>(@NonNull T value, @NonNull ReadToken token) {}
