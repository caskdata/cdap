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

package io.cdap.cdap.data.runtime.main;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import io.cdap.cdap.common.twill.AbstractMasterTwillRunnable;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.guice.KafkaLogAppenderModule;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.metrics.guice.MetricsProcessorStatusServiceModule;
import io.cdap.cdap.metrics.guice.MetricsStoreModule;
import io.cdap.cdap.metrics.process.MessagingMetricsProcessorServiceFactory;
import io.cdap.cdap.metrics.process.MetricsAdminSubscriberService;
import io.cdap.cdap.metrics.process.MetricsProcessorStatusService;
import io.cdap.cdap.metrics.process.loader.MetricsWriterModule;
import io.cdap.cdap.metrics.runtime.MessagingMetricsProcessorRuntimeService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Twill Runnable to run MetricsProcessor in YARN.
 */
public final class MetricsProcessorTwillRunnable extends AbstractMasterTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsProcessorTwillRunnable.class);

  private Injector injector;
  private int instanceId;

  public MetricsProcessorTwillRunnable(String name, String cConfName, String hConfName) {
    super(name, cConfName, hConfName);
  }

  @Override
  protected Injector doInit(TwillContext context) {
    getCConfiguration().set(Constants.MetricsProcessor.BIND_ADDRESS, context.getHost().getCanonicalHostName());
    // Set the hostname of the machine so that cConf can be used to start internal services
    LOG.info("{} Setting host name to {}", name, context.getHost().getCanonicalHostName());

    instanceId = context.getInstanceId();

    String txClientId = String.format("cdap.service.%s.%d", Constants.Service.METRICS_PROCESSOR,
                                      context.getInstanceId());
    injector = createGuiceInjector(getCConfiguration(), getConfiguration(), txClientId, context);

    injector.getInstance(LogAppenderInitializer.class).initialize();
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.METRICS_PROCESSOR));
    return injector;
  }

  @Override
  public void addServices(List<? super Service> services) {
    services.add(injector.getInstance(MessagingMetricsProcessorRuntimeService.class));
    services.add(injector.getInstance(MetricsProcessorStatusService.class));

    // Only starts the MetricsAdminSubscriberService in instance 0
    if (instanceId == 0) {
      services.add(injector.getInstance(MetricsAdminSubscriberService.class));
    }
  }

  @VisibleForTesting
  static Injector createGuiceInjector(CConfiguration cConf, Configuration hConf, String txClientId,
                                      TwillContext twillContext) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new ZKDiscoveryModule(),
      new KafkaClientModule(),
      new MessagingClientModule(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new MetricsStoreModule(),
      new KafkaLogAppenderModule(),
      new DFSLocationModule(),
      new NamespaceQueryAdminModule(),
      new DataFabricModules(txClientId).getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new SystemDatasetRuntimeModule().getDistributedModules(),
      new MetricsProcessorModule(twillContext),
      new MetricsProcessorStatusServiceModule(),
      new AuditModule(),
      new AuthorizationEnforcementModule().getDistributedModules(),
      new AuthenticationContextModules().getMasterModule(),
      new MetricsWriterModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
        }
      }
    );
  }

  static final class MetricsProcessorModule extends PrivateModule {
    final Integer instanceId;

    MetricsProcessorModule(TwillContext twillContext) {
      this.instanceId = twillContext.getInstanceId();
    }

    @Override
    protected void configure() {
      bind(Integer.class).annotatedWith(Names.named(Constants.Metrics.TWILL_INSTANCE_ID)).toInstance(instanceId);
      install(new FactoryModuleBuilder().build(MessagingMetricsProcessorServiceFactory.class));

      bind(MessagingMetricsProcessorRuntimeService.class);
      expose(MessagingMetricsProcessorRuntimeService.class);

      bind(MetricsAdminSubscriberService.class).in(Scopes.SINGLETON);
      expose(MetricsAdminSubscriberService.class);
    }
  }
}
