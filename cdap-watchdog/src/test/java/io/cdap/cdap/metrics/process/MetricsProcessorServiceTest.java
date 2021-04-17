/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.metrics.process;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import io.cdap.cdap.metrics.process.loader.MetricsWriterProvider;
import io.cdap.cdap.metrics.store.MetricDatasetFactory;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.tephra.TransactionManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Testing the basic properties of the {@link MessagingMetricsProcessorManagerService}
 */
public class MetricsProcessorServiceTest extends MetricsProcessorServiceTestBase {

  private static final String SYSTEM_METRIC_PREFIX = "system.";

  @Test
  public void testMetricsProcessor() throws Exception {
    injector.getInstance(TransactionManager.class).startAndWait();
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    structuredTableRegistry.initialize();
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class), structuredTableRegistry);
    injector.getInstance(DatasetOpExecutorService.class).startAndWait();
    injector.getInstance(DatasetService.class).startAndWait();

    final MetricStore metricStore = injector.getInstance(MetricStore.class);

    Set<Integer> partitions = new HashSet<>();
    for (int i = 0; i < cConf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM); i++) {
      partitions.add(i);
    }

    // Start KafkaMetricsProcessorService after metrics are published to Kafka

    // Intentionally set queue size to a small value, so that MessagingMetricsProcessorManagerService
    // internally can persist metrics when more messages are to be fetched
    MessagingMetricsProcessorManagerService messagingMetricsProcessorManagerService =
      new MessagingMetricsProcessorManagerService(cConf, injector.getInstance(MetricDatasetFactory.class),
                                                  messagingService, injector.getInstance(SchemaGenerator.class),
                                                  injector.getInstance(DatumReaderFactory.class),
                                                  metricStore, injector.getInstance(MetricsWriterProvider.class),
                                                  partitions, new NoopMetricsContext(), 50, 0);
    messagingMetricsProcessorManagerService.startAndWait();

    long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    // Publish metrics with messaging service and record expected metrics
    for (int i = 10; i < 20; i++) {
      publishMessagingMetrics(i, startTime, METRICS_CONTEXT, expected, SYSTEM_METRIC_PREFIX, MetricType.COUNTER);
    }

    Thread.sleep(500);
    // Stop and restart messagingMetricsProcessorManagerService
    messagingMetricsProcessorManagerService.stopAndWait();
    // Intentionally set queue size to a large value, so that MessagingMetricsProcessorManagerService
    // internally only persists metrics during terminating.
    messagingMetricsProcessorManagerService =
      new MessagingMetricsProcessorManagerService(cConf, injector.getInstance(MetricDatasetFactory.class),
                                                  messagingService, injector.getInstance(SchemaGenerator.class),
                                                  injector.getInstance(DatumReaderFactory.class),
                                                  metricStore, injector.getInstance(MetricsWriterProvider.class),
                                                  partitions, new NoopMetricsContext(), 50, 0);
    messagingMetricsProcessorManagerService.startAndWait();

    // Publish metrics after MessagingMetricsProcessorManagerService restarts and record expected metrics
    for (int i = 20; i < 30; i++) {
      publishMessagingMetrics(i, startTime, METRICS_CONTEXT, expected, SYSTEM_METRIC_PREFIX, MetricType.GAUGE);
    }

    final List<String> missingMetricNames = new ArrayList<>();
    // Wait until all expected metrics can be queried from the metric store. If not all expected metrics
    // are retrieved when timeout occurs, print out the missing metrics
    try {
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return canQueryAllMetrics(metricStore, METRICS_CONTEXT, expected, missingMetricNames);
        }
      }, 10000, TimeUnit.MILLISECONDS, "Failed to get all metrics");
    } catch (TimeoutException e) {
      Assert.fail(String.format("Metrics: [%s] cannot be found in the metrics store.",
                                Joiner.on(", ").join(missingMetricNames)));
    }

    // Query metrics from the metricStore and compare them with the expected ones
    assertMetricsResult(metricStore, METRICS_CONTEXT, expected);

    // Query for the 5 counter metrics published with messaging between time 5 - 14
    Collection<MetricTimeSeries> queryResult =
      metricStore.query(new MetricDataQuery(5, 14, 1, Integer.MAX_VALUE,
                                            ImmutableMap.of(SYSTEM_METRIC_PREFIX + COUNTER_METRIC_NAME,
                                                            AggregationFunction.SUM),
                                            METRICS_CONTEXT, ImmutableList.<String>of(), null));
    MetricTimeSeries timeSeries = Iterables.getOnlyElement(queryResult);
    Assert.assertEquals(5, timeSeries.getTimeValues().size());
    for (TimeValue timeValue : timeSeries.getTimeValues()) {
      Assert.assertEquals(1L, timeValue.getValue());
    }

    // Stop services and servers
    messagingMetricsProcessorManagerService.stopAndWait();
    // Delete all metrics
    metricStore.deleteAll();
  }

  /**
   * Checks whether all expected metrics can be obtained with query
   */
  private boolean canQueryAllMetrics(MetricStore metricStore, Map<String, String> metricsContext,
                                     Map<String, Long> expected, List<String> missingMetricNames) {
    missingMetricNames.clear();
    for (Map.Entry<String, Long> metric : expected.entrySet()) {
      Collection<MetricTimeSeries> queryResult =
        metricStore.query(new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE,
                                              metric.getKey(), AggregationFunction.SUM,
                                              metricsContext, ImmutableList.<String>of()));
      if (queryResult.size() == 0) {
        missingMetricNames.add(metric.getKey());
      }
    }
    return missingMetricNames.isEmpty();
  }

  private void assertMetricsResult(MetricStore metricStore, Map<String, String> metricsContext,
                                   Map<String, Long> expected) {
    for (Map.Entry<String, Long> metric : expected.entrySet()) {
      Collection<MetricTimeSeries> queryResult =
        metricStore.query(new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE,
                                              metric.getKey(), AggregationFunction.SUM,
                                              metricsContext, ImmutableList.<String>of()));
      MetricTimeSeries timeSeries = Iterables.getOnlyElement(queryResult);
      List<TimeValue> timeValues = timeSeries.getTimeValues();
      TimeValue timeValue = Iterables.getOnlyElement(timeValues);
      Assert.assertEquals(String.format("Actual value of metric: %s does not match expected", metric.getKey()),
                          metric.getValue().longValue(), timeValue.getValue());
    }
  }
}
