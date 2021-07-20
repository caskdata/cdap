/*
 * Copyright © 2014-2021 Cask Data, Inc.
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

package io.cdap.cdap.app.guice;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.mapreduce.DistributedMRJobInfoFetcher;
import io.cdap.cdap.app.mapreduce.LocalMRJobInfoFetcher;
import io.cdap.cdap.app.mapreduce.MRJobInfoFetcher;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.runtime.RuntimeModule;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.config.guice.ConfigStoreModule;
import io.cdap.cdap.data.security.DefaultSecretStore;
import io.cdap.cdap.gateway.handlers.AppLifecycleHttpHandler;
import io.cdap.cdap.gateway.handlers.AppLifecycleHttpHandlerInternal;
import io.cdap.cdap.gateway.handlers.ArtifactHttpHandler;
import io.cdap.cdap.gateway.handlers.ArtifactHttpHandlerInternal;
import io.cdap.cdap.gateway.handlers.AuthorizationHandler;
import io.cdap.cdap.gateway.handlers.BootstrapHttpHandler;
import io.cdap.cdap.gateway.handlers.CommonHandlers;
import io.cdap.cdap.gateway.handlers.ConfigHandler;
import io.cdap.cdap.gateway.handlers.ConsoleSettingsHttpHandler;
import io.cdap.cdap.gateway.handlers.FileFetcherHttpHandlerInternal;
import io.cdap.cdap.gateway.handlers.GitHubHttpHandler;
import io.cdap.cdap.gateway.handlers.ImpersonationHandler;
import io.cdap.cdap.gateway.handlers.InstanceOperationHttpHandler;
import io.cdap.cdap.gateway.handlers.NamespaceHttpHandler;
import io.cdap.cdap.gateway.handlers.OperationalStatsHttpHandler;
import io.cdap.cdap.gateway.handlers.OperationsDashboardHttpHandler;
import io.cdap.cdap.gateway.handlers.PreferencesHttpHandler;
import io.cdap.cdap.gateway.handlers.PreferencesHttpHandlerInternal;
import io.cdap.cdap.gateway.handlers.ProfileHttpHandler;
import io.cdap.cdap.gateway.handlers.ProgramLifecycleHttpHandler;
import io.cdap.cdap.gateway.handlers.ProgramLifecycleHttpHandlerInternal;
import io.cdap.cdap.gateway.handlers.ProvisionerHttpHandler;
import io.cdap.cdap.gateway.handlers.TransactionHttpHandler;
import io.cdap.cdap.gateway.handlers.UsageHandler;
import io.cdap.cdap.gateway.handlers.VersionHandler;
import io.cdap.cdap.gateway.handlers.WorkflowHttpHandler;
import io.cdap.cdap.gateway.handlers.WorkflowStatsSLAHttpHandler;
import io.cdap.cdap.gateway.handlers.meta.RemotePrivilegesHandler;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactoryProvider;
import io.cdap.cdap.internal.app.deploy.InMemoryConfigurator;
import io.cdap.cdap.internal.app.deploy.LocalApplicationManager;
import io.cdap.cdap.internal.app.deploy.RemoteConfigurator;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.namespace.DistributedStorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.LocalStorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactStore;
import io.cdap.cdap.internal.app.runtime.artifact.AuthorizationArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.DefaultArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.LocalArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.LocalPluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.schedule.DistributedTimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.ExecutorThreadPool;
import io.cdap.cdap.internal.app.runtime.schedule.LocalTimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.store.DatasetBasedTimeScheduleStore;
import io.cdap.cdap.internal.app.runtime.schedule.store.TriggerMisfireLogger;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowStateWriter;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import io.cdap.cdap.internal.app.services.LocalRunRecordCorrectorService;
import io.cdap.cdap.internal.app.services.NoopRunRecordCorrectorService;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.RunRecordCorrectorService;
import io.cdap.cdap.internal.app.services.ScheduledRunRecordCorrectorService;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.bootstrap.guice.BootstrapModules;
import io.cdap.cdap.internal.capability.CapabilityModule;
import io.cdap.cdap.internal.pipeline.SynchronousPipelineFactory;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.internal.provision.ProvisionerModule;
import io.cdap.cdap.internal.sysapp.SystemAppManagementService;
import io.cdap.cdap.metadata.LocalPreferencesFetcherInternal;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.pipeline.PipelineFactory;
import io.cdap.cdap.scheduler.CoreSchedulerService;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.securestore.spi.SecretStore;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.DefaultUGIProvider;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.impersonation.UnsupportedUGIProvider;
import io.cdap.cdap.security.store.SecureStoreHandler;
import io.cdap.http.HttpHandler;
import org.quartz.SchedulerException;
import org.quartz.core.JobRunShellFactory;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.DefaultThreadExecutor;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.StdJobRunShellFactory;
import org.quartz.impl.StdScheduler;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * AppFabric Service Runtime Module.
 */
public final class AppFabricServiceRuntimeModule extends RuntimeModule {
  public static final String NOAUTH_ARTIFACT_REPO = "noAuthArtifactRepo";

  private final CConfiguration cConf;

  @Inject
  public AppFabricServiceRuntimeModule(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public Module getInMemoryModules() {
    return Modules.combine(new AppFabricServiceModule(),
                           new CapabilityModule(),
                           new NamespaceAdminModule().getInMemoryModules(),
                           new ConfigStoreModule(),
                           new EntityVerifierModule(),
                           BootstrapModules.getInMemoryModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(RunRecordCorrectorService.class).to(NoopRunRecordCorrectorService.class)
                                 .in(Scopes.SINGLETON);
                               bind(TimeSchedulerService.class).to(LocalTimeSchedulerService.class)
                                 .in(Scopes.SINGLETON);
                               bind(MRJobInfoFetcher.class).to(LocalMRJobInfoFetcher.class);
                               bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);
                               bind(UGIProvider.class).toProvider(UGIProviderProvider.class);

                               Multibinder<String> servicesNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.services.names"));
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);

                               // TODO: Uncomment after CDAP-7688 is resolved
                               // servicesNamesBinder.addBinding().toInstance(Constants.Service.MESSAGING_SERVICE);

                               Multibinder<String> handlerHookNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.handler.hooks"));
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);

                               // TODO: Uncomment after CDAP-7688 is resolved
                               // handlerHookNamesBinder.addBinding().toInstance(Constants.Service.MESSAGING_SERVICE);
                             }
                           });
  }

  @Override
  public Module getStandaloneModules() {

    return Modules.combine(new AppFabricServiceModule(),
                           new CapabilityModule(),
                           new NamespaceAdminModule().getStandaloneModules(),
                           new ConfigStoreModule(),
                           new EntityVerifierModule(),
                           new ProvisionerModule(),
                           BootstrapModules.getFileBasedModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(RunRecordCorrectorService.class).to(LocalRunRecordCorrectorService.class)
                                 .in(Scopes.SINGLETON);
                               bind(TimeSchedulerService.class).to(LocalTimeSchedulerService.class)
                                 .in(Scopes.SINGLETON);
                               bind(MRJobInfoFetcher.class).to(LocalMRJobInfoFetcher.class);
                               bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);
                               bind(UGIProvider.class).toProvider(UGIProviderProvider.class);

                               Multibinder<String> servicesNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.services.names"));
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);

                               // for PingHandler
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.METRICS_PROCESSOR);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.LOGSAVER);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.TRANSACTION_HTTP);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.RUNTIME);

                               // TODO: Uncomment after CDAP-7688 is resolved
                               // servicesNamesBinder.addBinding().toInstance(Constants.Service.MESSAGING_SERVICE);

                               Multibinder<String> handlerHookNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.handler.hooks"));
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);

                               // for PingHandler
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.METRICS_PROCESSOR);
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.LOGSAVER);
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.TRANSACTION_HTTP);
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.RUNTIME);

                               // TODO: Uncomment after CDAP-7688 is resolved
                               // handlerHookNamesBinder.addBinding().toInstance(Constants.Service.MESSAGING_SERVICE);
                             }
                           });
  }

  @Override
  public Module getDistributedModules() {

    return Modules.combine(new AppFabricServiceModule(ImpersonationHandler.class),
                           new CapabilityModule(),
                           new NamespaceAdminModule().getDistributedModules(),
                           new ConfigStoreModule(),
                           new EntityVerifierModule(),
                           new ProvisionerModule(),
                           BootstrapModules.getFileBasedModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(RunRecordCorrectorService.class).to(ScheduledRunRecordCorrectorService.class)
                                 .in(Scopes.SINGLETON);
                               bind(TimeSchedulerService.class).to(DistributedTimeSchedulerService.class)
                                 .in(Scopes.SINGLETON);
                               bind(MRJobInfoFetcher.class).to(DistributedMRJobInfoFetcher.class);
                               bind(StorageProviderNamespaceAdmin.class)
                                 .to(DistributedStorageProviderNamespaceAdmin.class);
                               bind(UGIProvider.class).toProvider(UGIProviderProvider.class);

                               Multibinder<String> servicesNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.services.names"));
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.SECURE_STORE_SERVICE);

                               Multibinder<String> handlerHookNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.handler.hooks"));
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.SECURE_STORE_SERVICE);
                             }
                           });
  }

  /**
   * Guice module for AppFabricServer. Requires data-fabric related bindings being available.
   */
  private static final class AppFabricServiceModule extends AbstractModule {

    private final List<Class<? extends HttpHandler>> handlerClasses;

    private AppFabricServiceModule(Class<? extends HttpHandler>... handlerClasses) {
      this.handlerClasses = ImmutableList.copyOf(handlerClasses);
    }

    @Override
    protected void configure() {
      bind(PipelineFactory.class).to(SynchronousPipelineFactory.class);

      bind(PluginFinder.class).to(LocalPluginFinder.class);
      install(
        new FactoryModuleBuilder()
          .implement(new TypeLiteral<Manager<AppDeploymentInfo, ApplicationWithPrograms>>() {
                     },
                     new TypeLiteral<LocalApplicationManager<AppDeploymentInfo, ApplicationWithPrograms>>() {
                     })
          .build(new TypeLiteral<ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms>>() {
          })
      );

      install(
        new FactoryModuleBuilder()
          .implement(Configurator.class, InMemoryConfigurator.class)
          .build(Key.get(ConfiguratorFactory.class, Names.named("local")))
      );
      install(
        new FactoryModuleBuilder()
          .implement(Configurator.class, RemoteConfigurator.class)
          .build(Key.get(ConfiguratorFactory.class, Names.named("remote")))
      );

      bind(ConfiguratorFactory.class).toProvider(ConfiguratorFactoryProvider.class);

      bind(Store.class).to(DefaultStore.class);
      bind(SecretStore.class).to(DefaultSecretStore.class).in(Scopes.SINGLETON);

      // In App-Fabric, we can write directly, hence bind to the basic implementation
      bind(WorkflowStateWriter.class).to(BasicWorkflowStateWriter.class);

      bind(ArtifactStore.class).in(Scopes.SINGLETON);
      bind(ProfileService.class).in(Scopes.SINGLETON);
      bind(ProgramLifecycleService.class).in(Scopes.SINGLETON);
      bind(SystemAppManagementService.class).in(Scopes.SINGLETON);
      bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
      bind(CoreSchedulerService.class).in(Scopes.SINGLETON);
      bind(Scheduler.class).to(CoreSchedulerService.class);
      install(new PrivateModule() {
        @Override
        protected void configure() {
          // ArtifactRepositoryReader is required by DefaultArtifactRepository.
          // Keep ArtifactRepositoryReader private to minimize the scope of the binding visibility.
          bind(ArtifactRepositoryReader.class).to(LocalArtifactRepositoryReader.class).in(Scopes.SINGLETON);

          bind(ArtifactRepository.class)
            .annotatedWith(Names.named(NOAUTH_ARTIFACT_REPO))
            .to(DefaultArtifactRepository.class)
            .in(Scopes.SINGLETON);
          expose(ArtifactRepository.class).annotatedWith(Names.named(NOAUTH_ARTIFACT_REPO));

          bind(ArtifactRepository.class).to(AuthorizationArtifactRepository.class).in(Scopes.SINGLETON);
          expose(ArtifactRepository.class);
        }
      });
      bind(ProfileService.class).in(Scopes.SINGLETON);
      bind(PreferencesFetcher.class).to(LocalPreferencesFetcherInternal.class).in(Scopes.SINGLETON);

      Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(
        binder(), HttpHandler.class, Names.named(Constants.AppFabric.HANDLERS_BINDING));

      CommonHandlers.add(handlerBinder);
      handlerBinder.addBinding().to(ConfigHandler.class);
      handlerBinder.addBinding().to(VersionHandler.class);
      handlerBinder.addBinding().to(UsageHandler.class);
      handlerBinder.addBinding().to(GitHubHttpHandler.class);
      handlerBinder.addBinding().to(InstanceOperationHttpHandler.class);
      handlerBinder.addBinding().to(NamespaceHttpHandler.class);
      handlerBinder.addBinding().to(AppLifecycleHttpHandler.class);
      handlerBinder.addBinding().to(AppLifecycleHttpHandlerInternal.class);
      handlerBinder.addBinding().to(ProgramLifecycleHttpHandler.class);
      handlerBinder.addBinding().to(ProgramLifecycleHttpHandlerInternal.class);
      // TODO: [CDAP-13355] Move OperationsDashboardHttpHandler into report generation app
      handlerBinder.addBinding().to(OperationsDashboardHttpHandler.class);
      handlerBinder.addBinding().to(PreferencesHttpHandler.class);
      handlerBinder.addBinding().to(PreferencesHttpHandlerInternal.class);
      handlerBinder.addBinding().to(ConsoleSettingsHttpHandler.class);
      handlerBinder.addBinding().to(TransactionHttpHandler.class);
      handlerBinder.addBinding().to(WorkflowHttpHandler.class);
      handlerBinder.addBinding().to(ArtifactHttpHandler.class);
      handlerBinder.addBinding().to(ArtifactHttpHandlerInternal.class);
      handlerBinder.addBinding().to(WorkflowStatsSLAHttpHandler.class);
      handlerBinder.addBinding().to(AuthorizationHandler.class);
      handlerBinder.addBinding().to(SecureStoreHandler.class);
      handlerBinder.addBinding().to(RemotePrivilegesHandler.class);
      handlerBinder.addBinding().to(OperationalStatsHttpHandler.class);
      handlerBinder.addBinding().to(ProfileHttpHandler.class);
      handlerBinder.addBinding().to(ProvisionerHttpHandler.class);
      handlerBinder.addBinding().to(BootstrapHttpHandler.class);
      handlerBinder.addBinding().to(FileFetcherHttpHandlerInternal.class);

      for (Class<? extends HttpHandler> handlerClass : handlerClasses) {
        handlerBinder.addBinding().to(handlerClass);
      }
    }

    @Provides
    @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS)
    @SuppressWarnings("unused")
    public InetAddress providesHostname(CConfiguration cConf) {
      String address = cConf.get(Constants.Service.MASTER_SERVICES_BIND_ADDRESS);
      return Networks.resolve(address, new InetSocketAddress("localhost", 0).getAddress());
    }

    /**
     * Provides a supplier of quartz scheduler so that initialization of the scheduler can be done after guice
     * injection. It returns a singleton of Scheduler.
     */
    @Provides
    @SuppressWarnings("unused")
    public Supplier<org.quartz.Scheduler> providesSchedulerSupplier(final DatasetBasedTimeScheduleStore scheduleStore,
                                                                    final CConfiguration cConf) {
      return new Supplier<org.quartz.Scheduler>() {
        private org.quartz.Scheduler scheduler;

        @Override
        public synchronized org.quartz.Scheduler get() {
          try {
            if (scheduler == null) {
              scheduler = getScheduler(scheduleStore, cConf);
            }
            return scheduler;
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };
    }

    /**
     * Create a quartz scheduler. Quartz factory method is not used, because inflexible in allowing custom jobstore and
     * turning off check for new versions.
     *
     * @param store JobStore.
     * @param cConf CConfiguration.
     * @return an instance of {@link org.quartz.Scheduler}
     */
    private org.quartz.Scheduler getScheduler(JobStore store,
                                              CConfiguration cConf) throws SchedulerException {

      int threadPoolSize = cConf.getInt(Constants.Scheduler.CFG_SCHEDULER_MAX_THREAD_POOL_SIZE);
      ExecutorThreadPool threadPool = new ExecutorThreadPool(threadPoolSize);
      threadPool.initialize();
      String schedulerName = DirectSchedulerFactory.DEFAULT_SCHEDULER_NAME;
      String schedulerInstanceId = DirectSchedulerFactory.DEFAULT_INSTANCE_ID;

      QuartzSchedulerResources qrs = new QuartzSchedulerResources();
      JobRunShellFactory jrsf = new StdJobRunShellFactory();

      qrs.setName(schedulerName);
      qrs.setInstanceId(schedulerInstanceId);
      qrs.setJobRunShellFactory(jrsf);
      qrs.setThreadPool(threadPool);
      qrs.setThreadExecutor(new DefaultThreadExecutor());
      qrs.setJobStore(store);
      qrs.setRunUpdateCheck(false);
      QuartzScheduler qs = new QuartzScheduler(qrs, -1, -1);

      ClassLoadHelper cch = new CascadingClassLoadHelper();
      cch.initialize();

      store.initialize(cch, qs.getSchedulerSignaler());
      org.quartz.Scheduler scheduler = new StdScheduler(qs);

      jrsf.initialize(scheduler);
      qs.initialize();

      scheduler.getListenerManager().addTriggerListener(new TriggerMisfireLogger());
      return scheduler;
    }
  }

  /**
   * A Guice provider for the {@link UGIProvider} class based on the CDAP configuration.
   * <p>
   * When Kerberos is enabled, it provides {@link DefaultUGIProvider} instance. Otherwise, an {@link
   * UnsupportedUGIProvider} will be used.
   */
  private static final class UGIProviderProvider implements Provider<UGIProvider> {

    private final Injector injector;
    private final CConfiguration cConf;

    @Inject
    UGIProviderProvider(Injector injector, CConfiguration cConf) {
      this.injector = injector;
      this.cConf = cConf;
    }

    @Override
    public UGIProvider get() {
      if (SecurityUtil.isKerberosEnabled(cConf)) {
        return injector.getInstance(DefaultUGIProvider.class);
      }
      return injector.getInstance(UnsupportedUGIProvider.class);
    }
  }
}
