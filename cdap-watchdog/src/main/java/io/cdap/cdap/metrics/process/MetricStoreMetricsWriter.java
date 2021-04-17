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

import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.api.metrics.MetricsWriterContext;

import java.io.IOException;
import java.util.Collection;

/**
 * Metrics Forwarder for the MetricStore
 */
public class MetricStoreMetricsWriter implements MetricsWriter {

  private final MetricStore metricStore;

  public MetricStoreMetricsWriter(MetricStore metricStore) {
    this.metricStore = metricStore;

  }

  @Override
  public void write(Collection<MetricValues> metricValues) {
    this.metricStore.add(metricValues);
  }

  @Override
  public void initialize(MetricsWriterContext metricsWriterContext) {
    this.metricStore.setMetricsContext(metricsWriterContext.getMetricsContext());
  }

  @Override
  public String getID() {
    return "METRICS_STORE";
  }

  @Override
  public void close() throws IOException {
    //no-op
  }
}
