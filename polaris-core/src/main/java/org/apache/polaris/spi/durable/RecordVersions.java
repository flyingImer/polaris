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

/**
 * A record's versions, without the record.
 *
 * <p>Two versions rather than one, because they answer different questions and a caller commonly
 * needs only the second. {@link #recordVersion()} changes when the record itself is written; {@link
 * #grantRecordsVersion()} changes when a grant on or to the record changes. A cache holding an
 * entity's grant set needs to know the second moved without concluding the entity itself did.
 *
 * <p>The pair mirrors {@link Precondition.VersionAttribute}, so a caller can read a version here
 * and feed it straight into a condition without translating.
 *
 * @param recordVersion the record's own version
 * @param grantRecordsVersion the version of grants on or to this record; meaningful only for kinds
 *     that carry grants, and zero elsewhere
 */
public record RecordVersions(long recordVersion, long grantRecordsVersion) {}
