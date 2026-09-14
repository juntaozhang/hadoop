/*
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

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Pluggable classifier that decides whether an incoming RPC method is
 * read-only.
 *
 * <p>The implementation is intentionally kept out of the common IPC layer
 * so that {@code hadoop-common} does not need to depend on
 * protocol-specific modules such as {@code hadoop-hdfs-client}. A
 * protocol-specific module (e.g. HDFS) can provide an implementation that
 * inspects its own protocol interface.
 *
 * <p>When no classifier is configured, the IPC layer conservatively
 * treats every call as a write.
 */
@InterfaceAudience.Private
public interface ReadOnlyCallClassifier {

  /**
   * @param declaringClassProtocolName fully-qualified name of the
   *     protocol interface that declares the RPC method
   * @param methodName the RPC method name
   * @return {@code true} if the method is read-only
   */
  boolean isReadOnly(String declaringClassProtocolName, String methodName);
}
