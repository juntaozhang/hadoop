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
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.ipc.ReadOnlyCallClassifier;

/**
 * {@link ReadOnlyCallClassifier} implementation that classifies
 * {@link ClientProtocol} methods. The classification is based on their
 * {@link ReadOnly} annotation.
 *
 * <p>Method metadata is resolved once via reflection and cached, since
 * {@link ClientProtocol} has no overloaded methods.
 */
@InterfaceAudience.Private
public class ClientProtocolReadOnlyClassifier
    implements ReadOnlyCallClassifier {

  private static final Map<String, Boolean> READ_ONLY_METHODS;

  static {
    Map<String, Boolean> map = new HashMap<>();
    for (Method m : ClientProtocol.class.getDeclaredMethods()) {
      // TODO: methods annotated with @ReadOnly(atimeAffected = true), e.g.
      // getBlockLocations, may update the namespace access time when
      // dfs.namenode.accesstime.precision > 0. Those calls are effectively
      // writes to the namespace and should be classified as writes rather than
      // reads. Wire in the atime precision (or route them to the write queue)
      // once this read/write split needs to account for atime updates.
      map.put(m.getName(), m.isAnnotationPresent(ReadOnly.class));
    }
    READ_ONLY_METHODS = Collections.unmodifiableMap(map);
  }

  @Override
  public boolean isReadOnly(
      String declaringClassProtocolName, String methodName) {
    if (!ClientProtocol.class.getName().equals(declaringClassProtocolName)) {
      // Not a ClientProtocol method (e.g. service-RPC protocols); treat as
      // write so that classification stays conservative.
      return false;
    }
    Boolean readOnly = READ_ONLY_METHODS.get(methodName);
    return readOnly != null && readOnly;
  }
}
