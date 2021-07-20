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

package io.cdap.cdap.internal.app.runtime.distributed;

import ch.qos.logback.classic.Level;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramControllerCreator;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.common.app.MainClassLoader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.CConfigurationUtil;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.CombineClassLoader;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.logging.LoggerLogHandler;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.twill.TwillAppLifecycleEventHandler;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtilFactory;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.LocalizationUtils;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.master.spi.twill.SecretDisk;
import io.cdap.cdap.master.spi.twill.SecureTwillPreparer;
import io.cdap.cdap.master.spi.twill.SecurityContext;
import io.cdap.cdap.master.spi.twill.TwillConstants;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.store.SecureStoreUtils;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tephra.TxConstants;
import org.apache.twill.api.Configs;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;


/**
 * Defines the base framework for starting {@link Program} in the cluster.
 */
public abstract class DistributedProgramRunner implements ProgramRunner, ProgramControllerCreator {

  public static final String CDAP_CONF_FILE_NAME = "cConf.xml";
  public static final String APP_SPEC_FILE_NAME = "appSpec.json";
  public static final String PROGRAM_OPTIONS_FILE_NAME = "program.options.json";

  private static final Logger LOG = LoggerFactory.getLogger(DistributedProgramRunner.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .create();
  private static final String HADOOP_CONF_FILE_NAME = "hConf.xml";
  private static final String LOGBACK_FILE_NAME = "logback.xml";

  protected final CConfiguration cConf;
  protected final Configuration hConf;
  protected final ClusterMode clusterMode;
  private final TwillRunner twillRunner;
  private final Impersonator impersonator;

  protected DistributedProgramRunner(CConfiguration cConf, Configuration hConf, Impersonator impersonator,
                                     ClusterMode clusterMode, TwillRunner twillRunner) {
    this.twillRunner = twillRunner;
    this.hConf = hConf;
    this.cConf = cConf;
    this.impersonator = impersonator;
    this.clusterMode = clusterMode;
  }

  /**
   * Validates the options for the program.
   * Subclasses can override this to also validate the options for their sub-programs.
   */
  protected void validateOptions(Program program, ProgramOptions options) {
    // this will throw an exception if the custom tx timeout is invalid
    SystemArguments.validateTransactionTimeout(options.getUserArguments().asMap(), cConf);
  }

  /**
   * Provides the configuration for launching an program container.
   *
   * @param launchConfig the {@link ProgramLaunchConfig} to setup
   * @param program the program to launch
   * @param options the program options
   * @param cConf the configuration for this launch
   * @param hConf the hadoop configuration for this launch
   * @param tempDir a temporary directory for creating temp file. The content will be cleanup automatically
   *                once the program is launch.
   */
  protected abstract void setupLaunchConfig(ProgramLaunchConfig launchConfig, Program program, ProgramOptions options,
                                            CConfiguration cConf, Configuration hConf, File tempDir) throws IOException;

  @Override
  public final ProgramController run(final Program program, ProgramOptions oldOptions) {
    validateOptions(program, oldOptions);

    CConfiguration cConf = CConfiguration.copy(this.cConf);
    // Reload config for log extension jar update (CDAP-15091)
    cConf.reloadConfiguration();

    File tempDir = DirUtils.createTempDir(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                   cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());
    try {
      ProgramLaunchConfig launchConfig = new ProgramLaunchConfig();
      if (clusterMode == ClusterMode.ISOLATED) {
        // For isolated mode, the hadoop classes comes from the hadoop classpath in the target cluster directly
        launchConfig.addExtraClasspath(Collections.singletonList("$HADOOP_CLASSPATH"));
      }
      setupLaunchConfig(launchConfig, program, oldOptions, cConf, hConf, tempDir);

      // Add extra localize resources needed by the program runner
      final Map<String, LocalizeResource> localizeResources = new HashMap<>(launchConfig.getExtraResources());
      final List<String> additionalClassPaths = new ArrayList<>();
      addContainerJars(cConf, localizeResources, additionalClassPaths);
      addAdditionalLogAppenderJars(cConf, tempDir, localizeResources,
                                   SystemArguments.getProfileProvisioner(oldOptions.getArguments().asMap()));

      prepareHBaseDDLExecutorResources(tempDir, cConf, localizeResources);

      List<URI> configResources = localizeConfigs(createContainerCConf(cConf),
                                                  createContainerHConf(this.hConf), tempDir, localizeResources);

      // Localize the program jar
      Location programJarLocation = program.getJarLocation();
      final String programJarName = programJarLocation.getName();
      localizeResources.put(programJarName, new LocalizeResource(programJarLocation.toURI(), false));

      // Localize the app spec
      localizeResources.put(APP_SPEC_FILE_NAME,
                            new LocalizeResource(saveJsonFile(program.getApplicationSpecification(),
                                                              ApplicationSpecification.class,
                                                              File.createTempFile("appSpec", ".json", tempDir))));

      URI logbackURI = getLogBackURI(program);
      if (logbackURI != null) {
        // Localize the logback xml
        localizeResources.put(LOGBACK_FILE_NAME, new LocalizeResource(logbackURI, false));
      }

      // Update the ProgramOptions to carry program and runtime information necessary to reconstruct the program
      // and runs it in the remote container
      Map<String, String> extraSystemArgs = new HashMap<>(launchConfig.getExtraSystemArguments());
      extraSystemArgs.put(ProgramOptionConstants.PROGRAM_JAR, programJarName);
      extraSystemArgs.put(ProgramOptionConstants.HADOOP_CONF_FILE, HADOOP_CONF_FILE_NAME);
      extraSystemArgs.put(ProgramOptionConstants.CDAP_CONF_FILE, CDAP_CONF_FILE_NAME);
      extraSystemArgs.put(ProgramOptionConstants.APP_SPEC_FILE, APP_SPEC_FILE_NAME);

      ProgramOptions options = updateProgramOptions(oldOptions, localizeResources,
                                                    DirUtils.createTempDir(tempDir), extraSystemArgs);
      ProgramRunId programRunId = program.getId().run(ProgramRunners.getRunId(options));

      // Localize the serialized program options
      localizeResources.put(PROGRAM_OPTIONS_FILE_NAME,
                            new LocalizeResource(saveJsonFile(
                              options, ProgramOptions.class,
                              File.createTempFile("program.options", ".json", tempDir))));

      Callable<ProgramController> callable = () -> {
        ProgramTwillApplication twillApplication = new ProgramTwillApplication(
          programRunId, options, launchConfig.getRunnables(), launchConfig.getLaunchOrder(),
          localizeResources, createEventHandler(cConf, programRunId, options));

        TwillPreparer twillPreparer = twillRunner.prepare(twillApplication);

        // Also add the configuration files to container classpath so that the
        // TwillAppLifecycleEventHandler can get it. This can be removed when TWILL-246 is fixed.
        // Only ON_PREMISE mode will be using EventHandler
        twillPreparer.withResources(configResources);

        Map<String, String> userArgs = options.getUserArguments().asMap();

        // Setup log level
        twillPreparer.setLogLevels(transformLogLevels(SystemArguments.getLogLevels(userArgs)));

        // Set the configuration for the twill application
        Map<String, String> twillConfigs = new HashMap<>();
        if (DistributedProgramRunner.this instanceof LongRunningDistributedProgramRunner) {
          twillConfigs.put(Configs.Keys.YARN_ATTEMPT_FAILURES_VALIDITY_INTERVAL,
                           cConf.get(Constants.AppFabric.YARN_ATTEMPT_FAILURES_VALIDITY_INTERVAL));
        }
        // Add the one from the runtime arguments
        twillConfigs.putAll(SystemArguments.getTwillApplicationConfigs(userArgs));
        twillPreparer.withConfiguration(twillConfigs);

        // Setup per runnable configurations
        for (Map.Entry<String, RunnableDefinition> entry : launchConfig.getRunnables().entrySet()) {
          String runnable = entry.getKey();
          RunnableDefinition runnableDefinition = entry.getValue();
          if (runnableDefinition.getMaxRetries() != null) {
            twillPreparer.withMaxRetries(runnable, runnableDefinition.getMaxRetries());
          }
          twillPreparer.setLogLevels(runnable, transformLogLevels(runnableDefinition.getLogLevels()));
          twillPreparer.withConfiguration(runnable, runnableDefinition.getTwillRunnableConfigs());
          if (clusterMode == ClusterMode.ON_PREMISE && program.getType() == ProgramType.WORKFLOW) {
            // set TwillConstants#PROGRAM_RUNTIME_ENV configuration on twill preparer. This configuration will be
            // used when cdap programs are launched on k8s env.
            twillPreparer.withConfiguration(runnable,
                                            Collections.singletonMap(TwillConstants.PROGRAM_RUNTIME_ENV, "master"));
          }

          // Add cdap-security.xml if using secrets, and set the runnable identity.
          if (twillPreparer instanceof SecureTwillPreparer) {
            String twillSystemIdentity = cConf.get(Constants.Twill.Security.IDENTITY_SYSTEM);
            if (twillSystemIdentity != null) {
              SecurityContext securityContext = new SecurityContext.Builder()
                .withIdentity(twillSystemIdentity).build();
              twillPreparer = ((SecureTwillPreparer) twillPreparer).withSecurityContext(runnable, securityContext);
            }
            String securityName = cConf.get(Constants.Twill.Security.MASTER_SECRET_DISK_NAME);
            String securityPath = cConf.get(Constants.Twill.Security.MASTER_SECRET_DISK_PATH);
            twillPreparer = ((SecureTwillPreparer) twillPreparer)
              .withSecretDisk(runnable, new SecretDisk(securityName, securityPath));
          }
        }

        if (options.isDebug()) {
          twillPreparer.enableDebugging();
        }

        logProgramStart(program, options);

        // Add scheduler queue name if defined
        String schedulerQueueName = options.getArguments().getOption(Constants.AppFabric.APP_SCHEDULER_QUEUE);
        if (schedulerQueueName != null && !schedulerQueueName.isEmpty()) {
          LOG.info("Setting scheduler queue for app {} as {}", program.getId(), schedulerQueueName);
          twillPreparer.setSchedulerQueue(schedulerQueueName);
        }

        // Set JVM options based on configuration
        String jvmOpts = cConf.get(Constants.AppFabric.PROGRAM_JVM_OPTS);
        if (!Strings.isNullOrEmpty(jvmOpts)) {
          twillPreparer.addJVMOptions(jvmOpts);
        }

        if (logbackURI != null) {
          twillPreparer.addJVMOptions("-Dlogback.configurationFile=" + LOGBACK_FILE_NAME);
        }

        addLogHandler(twillPreparer, cConf);

        // Setup the environment for the container logback.xml
        twillPreparer.withEnv(Collections.singletonMap("CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR));

        // Add dependencies
        Set<Class<?>> extraDependencies = addExtraDependencies(cConf,
                                                               new HashSet<>(launchConfig.getExtraDependencies()));
        twillPreparer.withDependencies(extraDependencies);

        // Add the additional classes to the classpath that comes from the container jar setting
        twillPreparer.withClassPaths(additionalClassPaths);

        twillPreparer.withClassPaths(launchConfig.getExtraClasspath());
        twillPreparer.withEnv(launchConfig.getExtraEnv());

        // Add the YARN_APPLICATION_CLASSPATH so that yarn classpath are included in the twill container.
        // The Yarn app classpath goes last
        List<String> yarnAppClassPath = Arrays.asList(
          hConf.getTrimmedStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                  YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));
        twillPreparer
          .withApplicationClassPaths(yarnAppClassPath)
          .withClassPaths(yarnAppClassPath);

        twillPreparer
          .withBundlerClassAcceptor(launchConfig.getClassAcceptor())
          .withApplicationArguments(PROGRAM_OPTIONS_FILE_NAME)
          // Use the MainClassLoader for class rewriting
          .setClassLoader(MainClassLoader.class.getName());

        TwillController twillController;
        // Change the context classloader to the combine classloader of this ProgramRunner and
        // all the classloaders of the dependencies classes so that Twill can trace classes.
        ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(new CombineClassLoader(
          DistributedProgramRunner.this.getClass().getClassLoader(),
          extraDependencies.stream().map(Class::getClassLoader)::iterator));
        try {
          twillController = twillPreparer.start(cConf.getLong(Constants.AppFabric.PROGRAM_MAX_START_SECONDS),
                                                TimeUnit.SECONDS);

          // Block on the twill controller until it is in running state or terminated (due to failure)
          CountDownLatch latch = new CountDownLatch(1);
          twillController.onRunning(latch::countDown, Threads.SAME_THREAD_EXECUTOR);
          twillController.onTerminated(latch::countDown, Threads.SAME_THREAD_EXECUTOR);
          latch.await(cConf.getLong(Constants.AppFabric.PROGRAM_MAX_START_SECONDS), TimeUnit.SECONDS);
        } finally {
          ClassLoaders.setContextClassLoader(oldClassLoader);
        }

        return createProgramController(programRunId, addCleanupListener(twillController, program, tempDir));
      };

      return impersonator.doAs(programRunId, callable);

    } catch (Exception e) {
      deleteDirectory(tempDir);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Adds a {@link LocalizeResource} if extra log appender is being configured.
   */
  private void addAdditionalLogAppenderJars(CConfiguration cConf, File tempDir,
                                            Map<String, LocalizeResource> localizeResources,
                                            String provisioner) throws IOException {
    String provider = cConf.get(Constants.Logging.LOG_APPENDER_PROVIDER);
    if (Strings.isNullOrEmpty(provider)) {
      return;
    }
    // If the log appender provider doesn't support the provisioner for the program execution, unset the config.
    Collection<String> provisioners = cConf.getTrimmedStringCollection(Constants.Logging.LOG_APPENDER_PROVISIONERS);
    if (!provisioners.contains(provisioner)) {
      cConf.unset(Constants.Logging.LOG_APPENDER_PROVIDER);
      return;
    }

    LOG.debug("Configure log appender provider to {}", provider);

    String localizedDir = "log-appender-ext";
    File logAppenderArchive = new File(tempDir, localizedDir + ".zip");
    try (ZipOutputStream zipOut = new ZipOutputStream(Files.newOutputStream(logAppenderArchive.toPath()))) {
      BundleJarUtil.addToArchive(new File(cConf.get(Constants.Logging.LOG_APPENDER_EXT_DIR), provider), true, zipOut);
    }
    // set extensions dir to point to localized appender directory - appender/<log-appender-provider>
    localizeResources.put(localizedDir, new LocalizeResource(logAppenderArchive, true));
    cConf.set(Constants.Logging.LOG_APPENDER_EXT_DIR, localizedDir);
  }

  /**
   * Creates the {@link CConfiguration} to be used in the program container.
   */
  private CConfiguration createContainerCConf(CConfiguration cConf) {
    CConfiguration result = CConfiguration.copy(cConf);
    // don't have tephra retry in order to give CDAP more control over when to retry and how.
    result.set(TxConstants.Service.CFG_DATA_TX_CLIENT_RETRY_STRATEGY, "n-times");
    result.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_ATTEMPTS, 0);

    // Unset the hbase.client.retries.number and hbase.rpc.timeout so that program container
    // runs with default values for them from hbase-site/hbase-default.
    result.unset(Constants.HBase.CLIENT_RETRIES);
    result.unset(Constants.HBase.RPC_TIMEOUT);

    // Unset the runtime extension directory as the necessary extension jars should be shipped to the container
    // by the distributed ProgramRunner.
    result.unset(Constants.AppFabric.RUNTIME_EXT_DIR);

    // Set the CFG_LOCAL_DATA_DIR to a relative path as the data directory for the container should be relative to the
    // container directory
    result.set(Constants.CFG_LOCAL_DATA_DIR, "data");

    // Setup the configurations for isolated mode
    if (clusterMode == ClusterMode.ISOLATED) {
      // Disable logs collection through twill
      result.set(Constants.COLLECT_APP_CONTAINER_LOG_LEVEL, "OFF");

      // Disable implicit transaction
      result.set(Constants.AppFabric.PROGRAM_TRANSACTION_CONTROL, TransactionControl.EXPLICIT.name());

      // Disable transaction support
      result.setBoolean(Constants.Transaction.TX_ENABLED, false);

      // Disable explore
      result.set(Constants.Explore.EXPLORE_ENABLED, Boolean.FALSE.toString());

      // Always use NoSQL as storage
      result.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_NOSQL);

      // The following services will be running in the edge node host.
      // Set the bind addresses for all of them to "${master.services.bind.address}"
      // Which the value of `"master.services.bind.address" will be set in the DefaultRuntimeJob when it
      // starts in the remote machine
      String masterBindAddrConf = "${" + Constants.Service.MASTER_SERVICES_BIND_ADDRESS + "}";
      result.set(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS, masterBindAddrConf);

      // Don't try and use the CDAP system certs for SSL
      result.unset(Constants.Security.SSL.INTERNAL_CERT_PATH);
    }

    return result;
  }

  /**
   * Creates the {@link Configuration} to be used in the program container.
   */
  private Configuration createContainerHConf(Configuration hConf) {
    Configuration result = new YarnConfiguration(hConf);
    // Unset the hbase.client.retries.number and hbase.rpc.timeout so that program container
    // runs with default values for them from hbase-site/hbase-default.
    result.unset(Constants.HBase.CLIENT_RETRIES);
    result.unset(Constants.HBase.RPC_TIMEOUT);

    return result;
  }

  /**
   * Adds extra jars as defined in {@link CConfiguration} to the container and container classpath.
   */
  private void addContainerJars(CConfiguration cConf,
                                Map<String, LocalizeResource> localizeResources, Collection<String> classpath) {
    // Create localize resources and update classpath for the container based on the "program.container.dist.jars"
    // configuration.
    List<String> containerExtraJars = new ArrayList<>();
    for (URI jarURI : CConfigurationUtil.getExtraJars(cConf)) {
      String scheme = jarURI.getScheme();
      LocalizeResource localizeResource = new LocalizeResource(jarURI, false);
      String localizedName = LocalizationUtils.getLocalizedName(jarURI);
      localizeResources.put(localizedName, localizeResource);
      classpath.add(localizedName);
      String jarPath = "file".equals(scheme) ? localizedName : jarURI.toString();
      containerExtraJars.add(jarPath);
    }

    // Set the "program.container.dist.jars" since files are already localized to the container.
    cConf.setStrings(Constants.AppFabric.PROGRAM_CONTAINER_DIST_JARS, containerExtraJars.toArray(new String[0]));
  }

  /**
   * Set up Logging Context so the Log is tagged correctly for the Program.
   * Reset the context once done.
   */
  private void logProgramStart(Program program, ProgramOptions options) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContext(program.getNamespaceId(), program.getApplicationId(),
                                             program.getName(), program.getType(),
                                             ProgramRunners.getRunId(options).getId(),
                                             options.getArguments().asMap());
    Cancellable saveContextCancellable =
      LoggingContextAccessor.setLoggingContext(loggingContext);
    String userArguments = Joiner.on(", ").withKeyValueSeparator("=").join(options.getUserArguments());

    LOG.info("Starting {} Program '{}' with Arguments [{}], with debugging {}",
             program.getType(), program.getName(), userArguments, options.isDebug());
    saveContextCancellable.cancel();
  }

  /**
   * Creates a new instance of {@link ProgramOptions} with artifact localization information and with
   * extra system arguments, while maintaining other fields of the given {@link ProgramOptions}.
   *
   * @param options the original {@link ProgramOptions}.
   * @param localizeResources a {@link Map} of {@link LocalizeResource} to be localized to the remote container
   * @param tempDir a local temporary directory for creating files for artifact localization.
   * @param extraSystemArgs a set of extra system arguments to be added/updated
   * @return a new instance of {@link ProgramOptions}
   * @throws IOException if failed to create local copy of artifact files
   */
  private ProgramOptions updateProgramOptions(ProgramOptions options,
                                              Map<String, LocalizeResource> localizeResources,
                                              File tempDir, Map<String, String> extraSystemArgs) throws IOException {
    Arguments systemArgs = options.getArguments();

    Map<String, String> newSystemArgs = new HashMap<>(systemArgs.asMap());
    newSystemArgs.putAll(extraSystemArgs);

    String artifactArchiveJarName = "artifacts_archive.jar";
    String artifactDirName = "artifacts";

    if (systemArgs.hasOption(ProgramOptionConstants.PLUGIN_ARCHIVE)) {
      // If the archive already exists locally, we just need to re-localize it to remote containers
      File archiveFile = new File(systemArgs.getOption(ProgramOptionConstants.PLUGIN_ARCHIVE));
      // Localize plugins to two files, one expanded into a directory, one not.
      localizeResources.put(artifactDirName, new LocalizeResource(archiveFile, true));
      localizeResources.put(artifactArchiveJarName, new LocalizeResource(archiveFile, false));
    } else if (systemArgs.hasOption(ProgramOptionConstants.PLUGIN_DIR)) {
      // If there is a plugin directory, then we need to create an archive and localize it to remote containers
      File localDir = new File(systemArgs.getOption(ProgramOptionConstants.PLUGIN_DIR));
      File archiveFile = new File(tempDir, artifactDirName + ".jar");

      // Store all artifact jars into a new jar file for localization without compression
      try (JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(archiveFile))) {
        jarOut.setLevel(0);
        BundleJarUtil.addToArchive(localDir, jarOut);
      }

      // Localize plugins to two files, one expanded into a directory, one not.
      localizeResources.put(artifactDirName, new LocalizeResource(archiveFile, true));
      localizeResources.put(artifactArchiveJarName, new LocalizeResource(archiveFile, false));
    }

    // Add/rename the entries in the system arguments
    if (localizeResources.containsKey(artifactDirName)) {
      newSystemArgs.put(ProgramOptionConstants.PLUGIN_DIR, artifactDirName);
    }
    if (localizeResources.containsKey(artifactArchiveJarName)) {
      newSystemArgs.put(ProgramOptionConstants.PLUGIN_ARCHIVE, artifactArchiveJarName);
    }

    return new SimpleProgramOptions(options.getProgramId(), new BasicArguments(newSystemArgs),
                                    options.getUserArguments(), options.isDebug());
  }

  /**
   * Returns a {@link URI} for the logback.xml file to be localized to container and available in the container
   * classpath.
   */
  @Nullable
  private URI getLogBackURI(Program program) throws URISyntaxException {
    String configurationFile = System.getProperty("logback.configurationFile");
    if (configurationFile != null) {
      return new File(configurationFile).toURI();
    }

    URL logbackURL = program.getClassLoader().getResource("logback.xml");
    if (logbackURL != null) {
      return logbackURL.toURI();
    }

    for (String name : Arrays.asList("logback-container.xml", "logback.xml")) {
      URL resource = getClass().getClassLoader().getResource(name);
      if (resource != null) {
        return resource.toURI();
      }
    }

    return null;
  }

  private Map<String, LogEntry.Level> transformLogLevels(Map<String, Level> logLevels) {
    return logLevels.entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> convertLogLevel(e.getValue())));
  }

  /**
   * Converts a logback {@link Level} into twill {@link LogEntry.Level}.
   */
  private LogEntry.Level convertLogLevel(Level level) {
    // Twill LogEntry.Level doesn't have ALL and OFF, so map them to lowest and highest log level respectively
    if (level.equals(Level.ALL)) {
      return LogEntry.Level.TRACE;
    }
    if (level.equals(Level.OFF)) {
      return LogEntry.Level.FATAL;
    }
    return LogEntry.Level.valueOf(level.toString());
  }

  private File saveCConf(CConfiguration cConf, File file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      cConf.writeXml(writer);
    }
    return file;
  }

  private File saveHConf(Configuration conf, File file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      conf.writeXml(writer);
    }
    return file;
  }

  private <T> File saveJsonFile(T obj, Class<T> type, File file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      GSON.toJson(obj, type, writer);
    }
    return file;
  }

  /**
   * Deletes the given directory recursively. Only log if there is {@link IOException}.
   */
  private void deleteDirectory(File directory) {
    try {
      DirUtils.deleteDirectoryContents(directory);
    } catch (IOException e) {
      LOG.warn("Failed to delete directory {}", directory, e);
    }
  }

  /**
   * Adds a listener to the given TwillController to delete local temp files when the program has started/terminated.
   * The local temp files could be removed once the program is started, since Twill would keep the files in
   * HDFS and no long needs the local temp files once program is started.
   *
   * @return The same TwillController instance.
   */
  private TwillController addCleanupListener(TwillController controller,
                                             final Program program, final File tempDir) {

    final AtomicBoolean deleted = new AtomicBoolean(false);
    Runnable cleanup = () -> {
      if (!deleted.compareAndSet(false, true)) {
        return;
      }
      LOG.debug("Cleanup tmp files for {}: {}", program.getId(), tempDir);
      deleteDirectory(tempDir);
    };
    controller.onRunning(cleanup, Threads.SAME_THREAD_EXECUTOR);
    controller.onTerminated(cleanup, Threads.SAME_THREAD_EXECUTOR);
    return controller;
  }

  /**
   * Prepares the {@link HBaseDDLExecutor} implementation for localization.
   */
  private void prepareHBaseDDLExecutorResources(File tempDir, CConfiguration cConf,
                                                Map<String, LocalizeResource> localizeResources) throws IOException {
    String ddlExecutorExtensionDir = cConf.get(Constants.HBaseDDLExecutor.EXTENSIONS_DIR);

    // Only support HBase when running on premise
    if (ddlExecutorExtensionDir == null || clusterMode != ClusterMode.ON_PREMISE) {
      // Nothing to localize
      return;
    }

    final File target = new File(tempDir, "hbaseddlext.jar");
    BundleJarUtil.createJar(new File(ddlExecutorExtensionDir), target);
    localizeResources.put(target.getName(), new LocalizeResource(target, true));
    cConf.set(Constants.HBaseDDLExecutor.EXTENSIONS_DIR, target.getName());
  }

  /**
   * Creates the {@link EventHandler} for handling the application events.
   */
  private EventHandler createEventHandler(CConfiguration cConf, ProgramRunId programRunId, ProgramOptions programOpts) {
    return new TwillAppLifecycleEventHandler(cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE),
                                             this instanceof LongRunningDistributedProgramRunner, programRunId,
                                             clusterMode, SystemArguments.getRuntimeMonitorType(cConf, programOpts));
  }

  /**
   * Localizes the configuration files.
   *
   * @param cConf the CDAP configuration to localize
   * @param hConf the hadoop configuration to localize
   * @param tempDir a temporary directory for local file creation
   * @param localizeResources the {@link LocalizeResource} map to update
   * @return a list of {@link URI} that should be added as twill program resources.
   */
  private List<URI> localizeConfigs(CConfiguration cConf, Configuration hConf, File tempDir,
                                    Map<String, LocalizeResource> localizeResources) throws IOException {
    List<URI> resources = new ArrayList<>();

    File cConfFile = saveCConf(cConf, new File(tempDir, CDAP_CONF_FILE_NAME));
    localizeResources.put(CDAP_CONF_FILE_NAME, new LocalizeResource(cConfFile));
    resources.add(cConfFile.toURI());

    // Save the configuration to files
    File hConfFile = saveHConf(hConf, new File(tempDir, HADOOP_CONF_FILE_NAME));
    localizeResources.put(HADOOP_CONF_FILE_NAME, new LocalizeResource(hConfFile));
    resources.add(hConfFile.toURI());

    return resources;
  }

  /**
   * Adds a {@link LogHandler} to the {@link TwillPreparer} based on the configuration.
   */
  private void addLogHandler(TwillPreparer twillPreparer, CConfiguration cConf) {
    String confLevel = cConf.get(Constants.COLLECT_APP_CONTAINER_LOG_LEVEL).toUpperCase();
    if ("OFF".equals(confLevel)) {
      twillPreparer.withConfiguration(Collections.singletonMap(Configs.Keys.LOG_COLLECTION_ENABLED, "false"));
      return;
    }

    LogEntry.Level logLevel = LogEntry.Level.ERROR;
    try {
      logLevel = "ALL".equals(confLevel) ? LogEntry.Level.TRACE : LogEntry.Level.valueOf(confLevel.toUpperCase());
    } catch (Exception e) {
      LOG.warn("Invalid application container log level {}. Defaulting to ERROR.", confLevel);
    }
    twillPreparer.addLogHandler(new LoggerLogHandler(LOG, logLevel));
  }

  /**
   * Adds extra dependency classes based on the given configuration.
   */
  private Set<Class<?>> addExtraDependencies(CConfiguration cConf, Set<Class<?>> dependencies) {
    // Only support HBase and KMS when running on premise
    if (clusterMode == ClusterMode.ON_PREMISE) {
      try {
        dependencies.add(HBaseTableUtilFactory.getHBaseTableUtilClass(cConf));
      } catch (Exception e) {
        LOG.debug("No HBase dependencies to add.");
      }
      if (SecureStoreUtils.isKMSBacked(cConf) && SecureStoreUtils.isKMSCapable()) {
        dependencies.add(SecureStoreUtils.getKMSSecureStore());
      }
    }

    return dependencies;
  }
}
