/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metrics.guice.MetricsHandlerModule;
import io.cdap.cdap.metrics.guice.MetricsProcessorStatusServiceModule;
import io.cdap.cdap.metrics.guice.MetricsStoreModule;
import io.cdap.cdap.metrics.process.MessagingMetricsProcessorServiceFactory;
import io.cdap.cdap.metrics.process.MetricsAdminSubscriberService;
import io.cdap.cdap.metrics.process.MetricsProcessorStatusService;
import io.cdap.cdap.metrics.process.loader.MetricsWriterModule;
import io.cdap.cdap.metrics.query.MetricsQueryService;
import io.cdap.cdap.metrics.store.MetricsCleanUpService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

/**
 * The main class to run metrics services, which includes both metrics processor and metrics query.
 */
public class MetricsServiceMain extends AbstractServiceMain<EnvironmentOptions> {

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(MetricsServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv,
                                           EnvironmentOptions options, CConfiguration cConf) {
    return Arrays.asList(
      new NamespaceQueryAdminModule(),
      new AuthorizationEnforcementModule().getDistributedModules(),
      new AuthenticationContextModules().getMasterModule(),
      new MessagingClientModule(),
      new SystemDatasetRuntimeModule().getStandaloneModules(),
      new MetricsStoreModule(),
      new FactoryModuleBuilder().build(MessagingMetricsProcessorServiceFactory.class),
      new MetricsProcessorStatusServiceModule(),
      new MetricsHandlerModule(),
      new DFSLocationModule(),
      new MetricsWriterModule()
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources,
                             MasterEnvironment masterEnv, MasterEnvironmentContext masterEnvContext,
                             EnvironmentOptions options) {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    Set<Integer> topicNumbers = IntStream.range(0, cConf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM))
      .boxed()
      .collect(Collectors.toSet());

    MetricsContext metricsContext = injector.getInstance(MetricsCollectionService.class)
      .getContext(Constants.Metrics.METRICS_PROCESSOR_CONTEXT);

    services.add(injector.getInstance(MessagingMetricsProcessorServiceFactory.class)
                   .create(topicNumbers, metricsContext, 0));
    services.add(injector.getInstance(MetricsProcessorStatusService.class));
    services.add(injector.getInstance(MetricsQueryService.class));
    services.add(injector.getInstance(MetricsAdminSubscriberService.class));
    services.add(injector.getInstance(MetricsCleanUpService.class));
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext(EnvironmentOptions options) {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     Constants.Service.METRICS);
  }

  @Override
  protected void initializeDataSourceConnection(CConfiguration cConf) {
    // no-op since we use leveldb for metrics
  }
}
