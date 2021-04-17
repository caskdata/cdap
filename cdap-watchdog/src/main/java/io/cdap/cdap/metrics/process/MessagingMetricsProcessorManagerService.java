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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Metrics;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.metrics.process.loader.MetricsWriterProvider;
import io.cdap.cdap.metrics.store.MetricDatasetFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Manage MessagingMetricsProcessorServices with different MetricsWriters
 */
public class MessagingMetricsProcessorManagerService extends AbstractIdleService {

  private final List<MessagingMetricsProcessorService> metricsProcessorServices;
  private final List<MetricsWriter> metricsWriters;
  private final CConfiguration cConf;
  private final MetricDatasetFactory metricDatasetFactory;
  private final MessagingService messagingService;
  private final SchemaGenerator schemaGenerator;
  private final DatumReaderFactory readerFactory;
  private final MetricStore metricStore;
  private final MetricsWriterProvider metricsWriterProvider;
  private final Set<Integer> topicNumbers;
  private final MetricsContext metricsContext;
  private final long metricsProcessIntervalMillis;
  private final Integer instanceId;

  @Inject
  MessagingMetricsProcessorManagerService(CConfiguration cConf,
                                          MetricDatasetFactory metricDatasetFactory,
                                          MessagingService messagingService,
                                          SchemaGenerator schemaGenerator,
                                          DatumReaderFactory readerFactory,
                                          MetricStore metricStore,
                                          MetricsWriterProvider metricsWriterProvider,
                                          @Assisted Set<Integer> topicNumbers,
                                          @Assisted MetricsContext metricsContext,
                                          @Assisted Integer instanceId) {
    this(cConf, metricDatasetFactory, messagingService,
         schemaGenerator, readerFactory, metricStore, metricsWriterProvider, topicNumbers, metricsContext,
         TimeUnit.SECONDS.toMillis(cConf.getInt(Constants.Metrics.METRICS_MINIMUM_RESOLUTION_SECONDS)), instanceId);
  }

  @VisibleForTesting
  MessagingMetricsProcessorManagerService(CConfiguration cConf,
                                          MetricDatasetFactory metricDatasetFactory,
                                          MessagingService messagingService,
                                          SchemaGenerator schemaGenerator,
                                          DatumReaderFactory readerFactory,
                                          MetricStore metricStore,
                                          MetricsWriterProvider metricsWriterProvider,
                                          Set<Integer> topicNumbers,
                                          MetricsContext metricsContext,
                                          long metricsProcessIntervalMillis,
                                          int instanceId) {
    this.metricsWriters = new ArrayList<>();
    this.metricsProcessorServices = new ArrayList<>();
    this.cConf = cConf;
    this.metricDatasetFactory = metricDatasetFactory;
    this.messagingService = messagingService;
    this.schemaGenerator = schemaGenerator;
    this.readerFactory = readerFactory;
    this.metricStore = metricStore;
    this.metricsWriterProvider = metricsWriterProvider;
    this.topicNumbers = topicNumbers;
    this.metricsContext = metricsContext;
    this.metricsProcessIntervalMillis = metricsProcessIntervalMillis;
    this.instanceId = instanceId;
  }

  @Override
  protected void startUp() throws Exception {
    MetricStoreMetricsWriter metricsWriter = new MetricStoreMetricsWriter(metricStore);
    DefaultMetricsWriterContext context = new DefaultMetricsWriterContext(metricsContext,
      cConf, metricsWriter.getID());
    metricsWriter.initialize(context);
    this.metricsWriters.add(metricsWriter);

    for (Map.Entry<String, MetricsWriter> metricsWriterEntry : metricsWriterProvider.loadMetricsWriters().entrySet()) {
      MetricsWriter writer = metricsWriterEntry.getValue();
      this.metricsWriters.add(writer);
      DefaultMetricsWriterContext metricsWriterContext = new DefaultMetricsWriterContext(metricsContext,
        cConf, writer.getID());
      writer.initialize(metricsWriterContext);
    }

    for (MetricsWriter metricsExtension : this.metricsWriters) {
      metricsProcessorServices.add(new MessagingMetricsProcessorService(
        cConf,
        metricDatasetFactory,
        messagingService,
        schemaGenerator,
        readerFactory,
        metricsExtension,
        topicNumbers,
        metricsContext,
        metricsProcessIntervalMillis,
        instanceId));

    }

    for (MessagingMetricsProcessorService processorService : metricsProcessorServices) {
      processorService.startAndWait();
    }
  }

  @Override
  protected void shutDown() throws Exception {
    Exception exceptions = new Exception();
    for (MessagingMetricsProcessorService processorService : metricsProcessorServices) {
      try {
        processorService.stopAndWait();
        for (MetricsWriter metricsWriter : metricsWriters) {
          metricsWriter.close();
        }
      } catch (Exception e) {
        exceptions.addSuppressed(e);
      }
    }
    if (exceptions.getSuppressed().length > 0) {
      throw exceptions;
    }
  }
}

