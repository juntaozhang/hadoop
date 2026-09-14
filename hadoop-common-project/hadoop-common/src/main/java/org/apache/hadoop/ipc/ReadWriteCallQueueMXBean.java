/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ipc;

/** MXBean exposed by {@link ReadWriteCallQueue}. */
public interface ReadWriteCallQueueMXBean {
  /** Current number of read calls queued. */
  int getReadQueueSize();

  /** Current number of write calls queued. */
  int getWriteQueueSize();

  /** Number of times the scheduler entered the read phase. */
  long getReadPhaseCount();

  /** Number of times the scheduler entered the write phase. */
  long getWritePhaseCount();

  /** Cumulative number of read calls inserted into the read sub-queue. */
  long getReadInsertCount();

  /** Cumulative number of write calls inserted into the write sub-queue. */
  long getWriteInsertCount();

  /** Number of free capacity slots (== remainingCapacity). */
  int getSlotsAvailable();

  /** Revision number bumped whenever the underlying queue is replaced. */
  int getRevision();
}
