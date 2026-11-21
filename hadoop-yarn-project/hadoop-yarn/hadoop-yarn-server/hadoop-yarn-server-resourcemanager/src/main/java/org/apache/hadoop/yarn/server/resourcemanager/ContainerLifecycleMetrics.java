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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.apache.hadoop.metrics2.lib.Interns.info;

import java.util.LinkedHashSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Metrics for container lifecycle durations recorded by the ResourceManager.
 */
@InterfaceAudience.Private
@Metrics(context = "yarn")
public final class ContainerLifecycleMetrics implements MetricsSource {
  private static final MetricsInfo RECORD_INFO =
      info("ContainerLifecycleMetrics",
          "Metrics for containers completed by the ResourceManager");

  private final MetricsRegistry registry;
  private final MutableRate containerLifecycle;
  private final MutableQuantiles[] containerLifecycleQuantiles;

  private static volatile ContainerLifecycleMetrics INSTANCE = null;

  public static ContainerLifecycleMetrics getMetrics() {
    return getMetrics(new YarnConfiguration());
  }

  public static ContainerLifecycleMetrics getMetrics(Configuration conf) {
    synchronized (ContainerLifecycleMetrics.class) {
      if (INSTANCE == null) {
        INSTANCE = new ContainerLifecycleMetrics(conf);
        DefaultMetricsSystem.instance().register(RECORD_INFO.name(),
            RECORD_INFO.description(), INSTANCE);
      }
      return INSTANCE;
    }
  }

  public static synchronized void destroy() {
    if (INSTANCE != null) {
      INSTANCE.stop();
      DefaultMetricsSystem.instance().unregisterSource(RECORD_INFO.name());
    }
    INSTANCE = null;
  }

  ContainerLifecycleMetrics(Configuration conf) {
    String configuredValue = conf.get(
        YarnConfiguration.RM_CONTAINER_LIFECYCLE_QUANTILE_INTERVALS,
        YarnConfiguration.DEFAULT_RM_CONTAINER_LIFECYCLE_QUANTILE_INTERVALS);
    String[] configuredIntervals = configuredValue.split(",");
    LinkedHashSet<Integer> uniqueIntervals = new LinkedHashSet<>();
    for (String configuredInterval : configuredIntervals) {
      int interval = Integer.parseInt(configuredInterval.trim());
      if (interval <= 0) {
        throw new IllegalArgumentException(
            "Container lifecycle quantile intervals must be positive");
      }
      uniqueIntervals.add(interval);
    }
    if (uniqueIntervals.isEmpty()) {
      throw new IllegalArgumentException(
          "Container lifecycle quantile intervals must not be empty");
    }
    registry = new MetricsRegistry(RECORD_INFO);
    containerLifecycle = registry.newRate("containerLifecycle",
        "Time from RM container creation to completion in milliseconds");
    containerLifecycleQuantiles = new MutableQuantiles[uniqueIntervals.size()];
    int index = 0;
    for (int interval : uniqueIntervals) {
      containerLifecycleQuantiles[index++] = registry.newQuantiles(
          "containerLifecycle" + interval + "s",
          "Container lifecycle in milliseconds", "Ops", "Millis", interval);
    }
  }

  public void addContainerLifecycle(long durationMillis) {
    containerLifecycle.add(durationMillis);
    for (MutableQuantiles quantiles : containerLifecycleQuantiles) {
      quantiles.add(durationMillis);
    }
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(RECORD_INFO), all);
  }

  void stop() {
    for (MutableQuantiles quantiles : containerLifecycleQuantiles) {
      quantiles.stop();
    }
  }
}
