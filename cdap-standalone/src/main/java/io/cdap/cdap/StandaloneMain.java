/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.MonitorHandlerModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.guice.RuntimeServerModule;
import io.cdap.cdap.app.preview.PreviewConfigModule;
import io.cdap.cdap.app.preview.PreviewHttpServer;
import io.cdap.cdap.app.preview.PreviewManagerModule;
import io.cdap.cdap.app.preview.PreviewRunnerManager;
import io.cdap.cdap.app.preview.PreviewRunnerManagerModule;
import io.cdap.cdap.app.store.ServiceStore;
import io.cdap.cdap.common.ServiceBindException;
import io.cdap.cdap.common.app.MainClassLoader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.io.URLConnections;
import io.cdap.cdap.common.logging.common.UncaughtExceptionHandler;
import io.cdap.cdap.common.startup.ConfigurationLogger;
import io.cdap.cdap.common.twill.NoopTwillRunnerService;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.OSDetector;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.explore.executor.ExploreExecutorService;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.explore.guice.ExploreRuntimeModule;
import io.cdap.cdap.explore.service.ExploreServiceUtils;
import io.cdap.cdap.gateway.router.NettyRouter;
import io.cdap.cdap.gateway.router.RouterModules;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeServer;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import io.cdap.cdap.logging.LoggingUtil;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.framework.LogPipelineLoader;
import io.cdap.cdap.logging.guice.LocalLogAppenderModule;
import io.cdap.cdap.logging.guice.LogQueryRuntimeModule;
import io.cdap.cdap.logging.guice.LogReaderRuntimeModules;
import io.cdap.cdap.logging.service.LogQueryService;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.server.MessagingHttpService;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.metadata.MetadataService;
import io.cdap.cdap.metadata.MetadataServiceModule;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.metrics.guice.MetricsHandlerModule;
import io.cdap.cdap.metrics.process.loader.MetricsWriterModule;
import io.cdap.cdap.metrics.query.MetricsQueryService;
import io.cdap.cdap.operations.OperationalStatsService;
import io.cdap.cdap.operations.guice.OperationalStatsModule;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizerInstantiator;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import io.cdap.cdap.security.guice.SecurityModules;
import io.cdap.cdap.security.server.ExternalAuthenticationServer;
import io.cdap.cdap.security.store.SecureStoreService;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.tephra.inmemory.InMemoryTransactionService;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

/**
 * Standalone Main.
 * NOTE: Use AbstractIdleService
 */
public class StandaloneMain {

  // Special keys in the CConfiguration to disable stuff. It's mainly used for unit-tests that start Standalone.
  public static final String DISABLE_UI = "standalone.disable.ui";

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneMain.class);

  private final Injector injector;
  private final UserInterfaceService userInterfaceService;
  private final NettyRouter router;
  private final MetricsQueryService metricsQueryService;
  private final LogQueryService logQueryService;
  private final AppFabricServer appFabricServer;
  private final ServiceStore serviceStore;
  private final MetricsCollectionService metricsCollectionService;
  private final LogAppenderInitializer logAppenderInitializer;
  private final InMemoryTransactionService txService;
  private final MetadataService metadataService;
  private final boolean securityEnabled;
  private final boolean sslEnabled;
  private final CConfiguration cConf;
  private final DatasetService datasetService;
  private final DatasetOpExecutorService datasetOpExecutorService;
  private final ExploreClient exploreClient;
  private final AuthorizerInstantiator authorizerInstantiator;
  private final MessagingService messagingService;
  private final OperationalStatsService operationalStatsService;
  private final TwillRunnerService remoteExecutionTwillRunnerService;
  private final MetadataSubscriberService metadataSubscriberService;
  private final LevelDBTableService levelDBTableService;
  private final SecureStoreService secureStoreService;
  private final PreviewHttpServer previewHttpServer;
  private final PreviewRunnerManager previewRunnerManager;
  private final MetadataStorage metadataStorage;
  private final RuntimeServer runtimeServer;

  private ExternalAuthenticationServer externalAuthenticationServer;
  private ExploreExecutorService exploreExecutorService;


  private StandaloneMain(List<Module> modules, CConfiguration cConf) {
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());
    this.cConf = cConf;

    injector = Guice.createInjector(modules);

    levelDBTableService = injector.getInstance(LevelDBTableService.class);
    messagingService = injector.getInstance(MessagingService.class);
    authorizerInstantiator = injector.getInstance(AuthorizerInstantiator.class);
    router = injector.getInstance(NettyRouter.class);
    metricsQueryService = injector.getInstance(MetricsQueryService.class);
    logQueryService = injector.getInstance(LogQueryService.class);
    appFabricServer = injector.getInstance(AppFabricServer.class);
    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    datasetService = injector.getInstance(DatasetService.class);
    datasetOpExecutorService = injector.getInstance(DatasetOpExecutorService.class);
    serviceStore = injector.getInstance(ServiceStore.class);
    operationalStatsService = injector.getInstance(OperationalStatsService.class);
    remoteExecutionTwillRunnerService = injector.getInstance(Key.get(TwillRunnerService.class,
                                                                     Constants.AppFabric.RemoteExecution.class));
    metadataSubscriberService = injector.getInstance(MetadataSubscriberService.class);
    previewHttpServer = injector.getInstance(PreviewHttpServer.class);
    previewRunnerManager = injector.getInstance(PreviewRunnerManager.class);
    metadataStorage = injector.getInstance(MetadataStorage.class);
    runtimeServer = injector.getInstance(RuntimeServer.class);

    if (cConf.getBoolean(Constants.Transaction.TX_ENABLED)) {
      txService = injector.getInstance(InMemoryTransactionService.class);
    } else {
      txService = null;
    }

    if (cConf.getBoolean(DISABLE_UI, false)) {
      userInterfaceService = null;
    } else {
      userInterfaceService = injector.getInstance(UserInterfaceService.class);
    }

    sslEnabled = cConf.getBoolean(Constants.Security.SSL.EXTERNAL_ENABLED);
    securityEnabled = cConf.getBoolean(Constants.Security.ENABLED);
    if (securityEnabled) {
      externalAuthenticationServer = injector.getInstance(ExternalAuthenticationServer.class);
    }

    boolean exploreEnabled = cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED);
    if (exploreEnabled) {
      ExploreServiceUtils.checkHiveSupport(cConf, getClass().getClassLoader());
      exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    }

    exploreClient = injector.getInstance(ExploreClient.class);
    metadataService = injector.getInstance(MetadataService.class);
    secureStoreService = injector.getInstance(SecureStoreService.class);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          shutDown();
        } catch (Throwable e) {
          LOG.error("Failed to shutdown", e);
          // Because shutdown hooks execute concurrently, the logger may be closed already: thus also print it.
          System.err.println("Failed to shutdown: " + e.getMessage());
          e.printStackTrace(System.err);
        }
      }
    });
  }

  /**
   * INTERNAL METHOD. Returns the guice injector of Standalone. It's for testing only. Use with extra caution.
   */
  @VisibleForTesting
  public Injector getInjector() {
    return injector;
  }

  /**
   * Start the service.
   */
  public void startUp() throws Exception {
    // Workaround for release of file descriptors opened by URLClassLoader - https://issues.cask.co/browse/CDAP-2841
    URLConnections.setDefaultUseCaches(false);

    ConfigurationLogger.logImportantConfig(cConf);

    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    // TODO: CDAP-7688, remove next line after the issue is resolved
    injector.getInstance(MessagingHttpService.class).startAndWait();

    if (txService != null) {
      txService.startAndWait();
    }
    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class),
                                    injector.getInstance(StructuredTableRegistry.class));
    metadataStorage.createIndex();

    metricsCollectionService.startAndWait();
    datasetOpExecutorService.startAndWait();
    datasetService.startAndWait();
    serviceStore.startAndWait();

    remoteExecutionTwillRunnerService.start();
    metadataSubscriberService.startAndWait();

    // Validate the logging pipeline configuration.
    // Do it explicitly as Standalone doesn't have a separate master check phase as the distributed does.
    new LogPipelineLoader(cConf).validate();
    // It is recommended to initialize log appender after datasetService is started,
    // since log appender instantiates a dataset.
    logAppenderInitializer.initialize();

    runtimeServer.startAndWait();
    Service.State state = appFabricServer.startAndWait();
    if (state != Service.State.RUNNING) {
      throw new Exception("Failed to start Application Fabric");
    }

    previewHttpServer.startAndWait();
    if (previewRunnerManager instanceof Service) {
      ((Service) previewRunnerManager).startAndWait();
    }

    metricsQueryService.startAndWait();
    logQueryService.startAndWait();
    router.startAndWait();

    if (userInterfaceService != null) {
      userInterfaceService.startAndWait();
    }

    if (securityEnabled) {
      externalAuthenticationServer.startAndWait();
    }

    if (exploreExecutorService != null) {
      exploreExecutorService.startAndWait();
    }
    metadataService.startAndWait();

    operationalStatsService.startAndWait();

    secureStoreService.startAndWait();

    String protocol = sslEnabled ? "https" : "http";
    int dashboardPort = sslEnabled ?
      cConf.getInt(Constants.Dashboard.SSL_BIND_PORT) :
      cConf.getInt(Constants.Dashboard.BIND_PORT);
    System.out.println("CDAP Sandbox started successfully.");
    System.out.printf("Connect to the CDAP UI at %s://%s:%d\n", protocol, "localhost", dashboardPort);
  }

  /**
   * Shutdown the service.
   */
  public void shutDown() {
    LOG.info("Shutting down Standalone CDAP");
    boolean halt = false;
    try {
      // order matters: first shut down UI 'cause it will stop working after router is down
      if (userInterfaceService != null) {
        userInterfaceService.stopAndWait();
      }

      //  shut down router to stop all incoming traffic
      router.stopAndWait();

      secureStoreService.stopAndWait();
      operationalStatsService.stopAndWait();

      // Stop all services that requires tx service
      metadataSubscriberService.stopAndWait();
      if (exploreExecutorService != null) {
        exploreExecutorService.stopAndWait();
      }
      exploreClient.close();
      metadataService.stopAndWait();
      remoteExecutionTwillRunnerService.stop();
      serviceStore.stopAndWait();
      if (previewRunnerManager instanceof Service) {
        ((Service) previewRunnerManager).stopAndWait();
      }
      previewHttpServer.stopAndWait();
      // app fabric will also stop all programs
      appFabricServer.stopAndWait();
      runtimeServer.stopAndWait();
      // all programs are stopped: dataset service, metrics, transactions can stop now
      datasetService.stopAndWait();
      datasetOpExecutorService.stopAndWait();

      logQueryService.stopAndWait();

      metricsCollectionService.stopAndWait();
      metricsQueryService.stopAndWait();

      if (txService != null) {
        txService.stopAndWait();
      }

      if (securityEnabled) {
        // auth service is on the side anyway
        externalAuthenticationServer.stopAndWait();
      }

      // TODO: CDAP-7688, remove next line after the issue is resolved
      injector.getInstance(MessagingHttpService.class).startAndWait();
      if (messagingService instanceof Service) {
        ((Service) messagingService).stopAndWait();
      }

      logAppenderInitializer.close();
      authorizerInstantiator.close();
      metadataStorage.close();
      levelDBTableService.close();
    } catch (Throwable e) {
      halt = true;
      LOG.error("Exception during shutdown", e);
    } finally {
      File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                             cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
      cleanupTempDir(tmpDir);
    }

    // We can't do much but exit. Because there was an exception, some non-daemon threads may still be running.
    // Therefore System.exit() won't do it, we need to force a halt.
    if (halt) {
      Runtime.getRuntime().halt(1);
    }
  }

  private static void cleanupTempDir(File tmpDir) {
    if (!tmpDir.isDirectory()) {
      return;
    }

    try {
      DirUtils.deleteDirectoryContents(tmpDir, true);
    } catch (IOException e) {
      // It's ok not able to cleanup temp directory.
      LOG.debug("Failed to cleanup temp directory {}", tmpDir, e);
    }
  }

  public static void main(String[] args) throws Exception {
    // Includes logging extension jars as part of the system classpath.
    // It is needed to support custom appenders loaded from those extension jars.
    ClassLoader classLoader = MainClassLoader.createFromContext(
      LoggingUtil.getExtensionJarsAsURLs(CConfiguration.create()));
    if (classLoader == null) {
      LOG.warn("Failed to create CDAP system ClassLoader. Lineage record and Audit Log will not be updated.");
      doMain(args);
    } else {
      Thread.currentThread().setContextClassLoader(classLoader);
      Class<?> cls = classLoader.loadClass(StandaloneMain.class.getName());
      cls.getDeclaredMethod("doMain", String[].class).invoke(null, new Object[]{args});
    }
  }

  /**
   * The actual main method. It is called using reflection from {@link #main(String[])}.
   */
  @SuppressWarnings("unused")
  public static void doMain(String[] args) {
    StandaloneMain main = create(CConfiguration.create(), new Configuration());
    try {
      if (args.length > 0) {
        System.out.printf("%s takes no arguments\n", StandaloneMain.class.getSimpleName());
        System.out.println("These arguments are being ignored:");
        for (int i = 0; i <= args.length - 1; i++) {
          System.out.printf("Parameter #%d: %s\n", i, args[i]);
        }
      }
      main.startUp();
    } catch (Throwable e) {
      @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
      Throwable rootCause = Throwables.getRootCause(e);
      if (rootCause instanceof ServiceBindException) {
        LOG.error("Failed to start Standalone CDAP: {}", rootCause.getMessage());
        System.err.println("Failed to start Standalone CDAP: " + rootCause.getMessage());
      } else {
        // exception stack trace will be logged by
        // UncaughtExceptionIdleService.UNCAUGHT_EXCEPTION_HANDLER
        LOG.error("Failed to start Standalone CDAP");
        System.err.println("Failed to start Standalone CDAP");
        e.printStackTrace(System.err);
      }
      Runtime.getRuntime().halt(-2);
    }
  }

  @VisibleForTesting
  static StandaloneMain create(CConfiguration cConf, Configuration hConf) {
    // This is needed to use LocalJobRunner with fixes (we have it in app-fabric).
    // For the modified local job runner
    hConf.addResource("mapred-site-local.xml");
    hConf.reloadConfiguration();
    // Due to incredibly stupid design of Limits class, once it is initialized, it keeps its settings. We
    // want to make sure it uses our settings in this hConf, so we have to force it initialize here before
    // someone else initializes it.
    Limits.init(hConf);

    File localDataDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR));
    hConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    hConf.set(Constants.AppFabric.OUTPUT_DIR, cConf.get(Constants.AppFabric.OUTPUT_DIR));
    hConf.set("hadoop.tmp.dir", new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsolutePath());

    // Windows specific requirements
    if (OSDetector.isWindows()) {
      // not set anywhere by the project, expected to be set from IDEs if running from the project instead of sdk
      // hadoop.dll is at cdap-unit-test\src\main\resources\hadoop.dll for some reason
      String hadoopDLLPath = System.getProperty("hadoop.dll.path");
      if (hadoopDLLPath != null) {
        System.load(hadoopDLLPath);
      } else {
        // this is where it is when the standalone sdk is built
        String userDir = System.getProperty("user.dir");
        System.load(Joiner.on(File.separator).join(userDir, "lib", "native", "hadoop.dll"));
      }
    }

    //Run dataset service on random port
    List<Module> modules = createPersistentModules(cConf, hConf);

    // Cleans up the temporary directory which might contain unnecessary files from previous CDAP standalone runs
    // This happens here instead of in startup because it needs to happen before Authorizer is created, see CDAP-17239
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    cleanupTempDir(tmpDir);

    return new StandaloneMain(modules, cConf);
  }

  private static List<Module> createPersistentModules(CConfiguration cConf, Configuration hConf) {
    cConf.setInt(Constants.Master.MAX_INSTANCES, 1);
    cConf.setIfUnset(Constants.CFG_DATA_LEVELDB_DIR, Constants.DEFAULT_DATA_LEVELDB_DIR);

    cConf.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.LEVELDB.name());

    // configure all services except for router and auth to bind to 127.0.0.1
    String localhost = InetAddress.getLoopbackAddress().getHostAddress();
    cConf.set(Constants.Service.MASTER_SERVICES_BIND_ADDRESS, localhost);
    cConf.set(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS, localhost);
    cConf.set(Constants.Transaction.Container.ADDRESS, localhost);
    cConf.set(Constants.Dataset.Executor.ADDRESS, localhost);
    cConf.set(Constants.Metrics.ADDRESS, localhost);
    cConf.set(Constants.MetricsProcessor.BIND_ADDRESS, localhost);
    cConf.set(Constants.LogSaver.ADDRESS, localhost);
    cConf.set(Constants.LogQuery.ADDRESS, localhost);
    cConf.set(Constants.Explore.SERVER_ADDRESS, localhost);
    cConf.set(Constants.Metadata.SERVICE_BIND_ADDRESS, localhost);
    cConf.set(Constants.Preview.ADDRESS, localhost);

    return ImmutableList.of(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new MetricsHandlerModule(),
      new LogQueryRuntimeModule().getStandaloneModules(),
      new InMemoryDiscoveryModule(),
      new LocalLocationModule(),
      new ProgramRunnerRuntimeModule().getStandaloneModules(),
      new DataFabricModules(StandaloneMain.class.getName()).getStandaloneModules(),
      new DataSetsModules().getStandaloneModules(),
      new DataSetServiceModules().getStandaloneModules(),
      new MetricsClientRuntimeModule().getStandaloneModules(),
      new LocalLogAppenderModule(),
      new LogReaderRuntimeModules().getStandaloneModules(),
      new RouterModules().getStandaloneModules(),
      new SecurityModules().getStandaloneModules(),
      new SecureStoreServerModule(),
      new ExploreRuntimeModule().getStandaloneModules(),
      new ExploreClientModule(),
      new MetadataServiceModule(),
      new MetadataReaderWriterModules().getStandaloneModules(),
      new AuditModule(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getStandaloneModules(),
      new PreviewConfigModule(cConf, new Configuration(), SConfiguration.create()),
      new PreviewManagerModule(false),
      new PreviewRunnerManagerModule().getStandaloneModules(),
      new MessagingServerRuntimeModule().getStandaloneModules(),
      new AppFabricServiceRuntimeModule().getStandaloneModules(),
      new MonitorHandlerModule(false),
      new RuntimeServerModule(),
      new OperationalStatsModule(),
      new MetricsWriterModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // Needed by MonitorHandlerModuler
          bind(TwillRunner.class).to(NoopTwillRunnerService.class);
        }
      }
    );
  }
}
