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

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getLongGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

import com.google.common.base.Supplier;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link ContainerLifecycleMetrics}.
 */
public class TestContainerLifecycleMetrics {
  private ContainerLifecycleMetrics metrics;

  @Before
  public void setup() {
    DefaultMetricsSystem.initialize("ResourceManager");
  }

  @After
  public void tearDown() {
    if (metrics != null) {
      metrics.stop();
    }
    ContainerLifecycleMetrics.destroy();
    DefaultMetricsSystem.shutdown();
  }

  @Test
  public void testContainerLifecycle() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_CONTAINER_LIFECYCLE_QUANTILE_INTERVALS, "1");
    metrics = new ContainerLifecycleMetrics(conf);
    metrics.addContainerLifecycle(100);
    metrics.addContainerLifecycle(200);
    metrics.addContainerLifecycle(300);

    assertCounter("ContainerLifecycleNumOps", 3L, getMetrics(metrics));
    assertGauge("ContainerLifecycleAvgTime", 200.0, getMetrics(metrics));

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          assertGauge("ContainerLifecycle1sNumOps", 3L, getMetrics(metrics));
          return true;
        } catch (AssertionError e) {
          return false;
        }
      }
    }, 100, 3000);

    org.junit.Assert.assertTrue(getLongGauge(
        "ContainerLifecycle1s50thPercentileMillis", getMetrics(metrics)) >= 0);
    org.junit.Assert.assertTrue(getLongGauge(
        "ContainerLifecycle1s99thPercentileMillis", getMetrics(metrics)) >= 0);
  }

  @Test
  public void testConfiguredQuantileIntervals() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_CONTAINER_LIFECYCLE_QUANTILE_INTERVALS,
        "2,3");
    metrics = new ContainerLifecycleMetrics(conf);
    metrics.addContainerLifecycle(100);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          assertGauge("ContainerLifecycle2sNumOps", 1L, getMetrics(metrics));
          assertGauge("ContainerLifecycle3sNumOps", 1L, getMetrics(metrics));
          return true;
        } catch (AssertionError e) {
          return false;
        }
      }
    }, 100, 5000);

    org.junit.Assert.assertTrue(getLongGauge(
        "ContainerLifecycle2s50thPercentileMillis", getMetrics(metrics)) >= 0);
    org.junit.Assert.assertTrue(getLongGauge(
        "ContainerLifecycle3s50thPercentileMillis", getMetrics(metrics)) >= 0);
  }

  @Test
  public void testDefaultQuantileIntervals() {
    metrics = new ContainerLifecycleMetrics(new YarnConfiguration());
    for (int interval : new int[]{60, 300, 3600}) {
      org.junit.Assert.assertTrue(getLongGauge(
          "ContainerLifecycle" + interval + "sNumOps",
          getMetrics(metrics)) >= 0);
    }
  }

  @Test
  public void testSingletonReinitialization() {
    ContainerLifecycleMetrics first = ContainerLifecycleMetrics.getMetrics();
    ContainerLifecycleMetrics.destroy();
    ContainerLifecycleMetrics second = ContainerLifecycleMetrics.getMetrics();
    org.junit.Assert.assertNotSame(first, second);
  }

  @Test
  public void testSingletonUsesProvidedConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_CONTAINER_LIFECYCLE_QUANTILE_INTERVALS, "7");
    metrics = ContainerLifecycleMetrics.getMetrics(conf);
    org.junit.Assert.assertTrue(getLongGauge("ContainerLifecycle7sNumOps",
        getMetrics(metrics)) >= 0);
  }

  @Test
  public void testInvalidQuantileIntervals() {
    for (String value : new String[]{"", "invalid", "60,0", "-1"}) {
      YarnConfiguration conf = new YarnConfiguration();
      conf.set(YarnConfiguration.RM_CONTAINER_LIFECYCLE_QUANTILE_INTERVALS,
          value);
      try {
        new ContainerLifecycleMetrics(conf);
        org.junit.Assert.fail("Invalid interval should be rejected: " + value);
      } catch (IllegalArgumentException expected) {
        // Expected invalid configuration.
      }
    }
  }
}
