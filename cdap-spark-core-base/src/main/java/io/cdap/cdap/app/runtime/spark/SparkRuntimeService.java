/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.ProgramState;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.spark.Spark;
import io.cdap.cdap.api.spark.SparkClientContext;
import io.cdap.cdap.app.runtime.spark.distributed.SparkContainerLauncher;
import io.cdap.cdap.app.runtime.spark.python.PySparkUtil;
import io.cdap.cdap.app.runtime.spark.submit.SparkSubmitter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.CConfigurationUtil;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.PropertyFieldSetter;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.transaction.Transactions;
import io.cdap.cdap.internal.app.runtime.DataSetFieldSetter;
import io.cdap.cdap.internal.app.runtime.LocalizationUtils;
import io.cdap.cdap.internal.app.runtime.MetricsFieldSetter;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.batch.distributed.ContainerLauncherGenerator;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.internal.lang.Fields;
import io.cdap.cdap.internal.lang.Reflections;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.twill.api.Configs;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.Deflater;
import javax.annotation.Nullable;

/**
 * Performs the actual execution of Spark job.
 * <p/>
 * Service start -> Performs job setup, and initialize call.
 * Service run -> Submits the spark job through {@link SparkSubmit}
 * Service triggerStop -> kill job
 * Service stop -> Commit/invalidate transaction, destroy, cleanup
 */
final class SparkRuntimeService extends AbstractExecutionThreadService {

  private static final String CDAP_LAUNCHER_JAR = "cdap-spark-launcher.jar";
  private static final String CDAP_SPARK_JAR = "cdap-spark.jar";
  private static final String CDAP_METRICS_PROPERTIES = "metrics.properties";
  private static final Function<File, URI> FILE_TO_URI = new Function<File, URI>() {
    @Override
    public URI apply(File input) {
      return input.toURI();
    }
  };

  private static final Logger LOG = LoggerFactory.getLogger(SparkRuntimeService.class);

  private final CConfiguration cConf;
  private final Spark spark;
  private final SparkRuntimeContext runtimeContext;
  private final File pluginArchive;
  private final SparkSubmitter sparkSubmitter;
  private final LocationFactory locationFactory;
  private final AtomicReference<ListenableFuture<RunId>> completion;
  private final BasicSparkClientContext context;
  private final boolean isLocal;
  private final ProgramLifecycle<SparkRuntimeContext> programLifecycle;
  private final FieldLineageWriter fieldLineageWriter;

  private Callable<ListenableFuture<RunId>> submitSpark;
  private Runnable cleanupTask;

  SparkRuntimeService(CConfiguration cConf, final Spark spark, @Nullable File pluginArchive,
                      SparkRuntimeContext runtimeContext, SparkSubmitter sparkSubmitter,
                      LocationFactory locationFactory, boolean isLocal, FieldLineageWriter fieldLineageWriter) {
    this.cConf = cConf;
    this.spark = spark;
    this.runtimeContext = runtimeContext;
    this.pluginArchive = pluginArchive;
    this.sparkSubmitter = sparkSubmitter;
    this.locationFactory = locationFactory;
    this.completion = new AtomicReference<>();
    this.context = new BasicSparkClientContext(runtimeContext);
    this.isLocal = isLocal;
    this.programLifecycle = new ProgramLifecycle<SparkRuntimeContext>() {
      @Override
      public void initialize(SparkRuntimeContext runtimeContext) throws Exception {
        // Use the BasicSparkClientContext to call user method.
        if (spark instanceof ProgramLifecycle) {
          ((ProgramLifecycle) spark).initialize(context);
        }
      }

      @Override
      public void destroy() {
        if (spark instanceof ProgramLifecycle) {
          ((ProgramLifecycle) spark).destroy();
        }
      }
    };
    this.fieldLineageWriter = fieldLineageWriter;
  }

  @Override
  protected String getServiceName() {
    return "Spark - " + runtimeContext.getSparkSpecification().getName();
  }

  @Override
  protected void startUp() throws Exception {
    // additional spark job initialization at run-time
    // This context is for calling initialize and onFinish on the Spark program

    // Fields injection for the Spark program
    // It has to be done in here instead of in SparkProgramRunner for the @UseDataset injection
    // since the dataset cache being used in Spark is a MultiThreadDatasetCache
    // The AbstractExecutionThreadService guarantees that startUp(), run() and shutDown() all happens in the same thread
    Reflections.visit(spark, spark.getClass(),
                      new PropertyFieldSetter(runtimeContext.getSparkSpecification().getProperties()),
                      new DataSetFieldSetter(runtimeContext.getDatasetCache()),
                      new MetricsFieldSetter(runtimeContext));

    // Since we are updating cConf, make a copy of it to which updates will be made.
    CConfiguration cConfCopy = CConfiguration.copy(cConf);
    // Creates a temporary directory locally for storing all generated files.
    File tempDir = DirUtils.createTempDir(new File(cConfCopy.get(Constants.CFG_LOCAL_DATA_DIR),
                                                   cConfCopy.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());
    tempDir.mkdirs();
    this.cleanupTask = createCleanupTask(tempDir, System.getProperties());
    try {
      initialize();
      SparkRuntimeContextConfig contextConfig = new SparkRuntimeContextConfig(runtimeContext.getConfiguration());

      final List<LocalizeResource> localizeResources = new ArrayList<>();
      final URI jobFile = context.isPySpark() ? getPySparkScript(tempDir) : createJobJar(tempDir);
      List<File> extraPySparkFiles = new ArrayList<>();

      String metricsConfPath;
      String classpath = "";

      // Setup the SparkConf with properties from spark-defaults.conf
      Properties sparkDefaultConf = SparkPackageUtils.getSparkDefaultConf();
      for (String key : sparkDefaultConf.stringPropertyNames()) {
        SparkRuntimeEnv.setProperty(key, sparkDefaultConf.getProperty(key));
      }

      if (isLocal) {
        // In local mode, always copy (or link if local) user requested resources
        copyUserResources(context.getLocalizeResources(), tempDir);

        File metricsConf = SparkMetricsSink.writeConfig(new File(tempDir, CDAP_METRICS_PROPERTIES));
        metricsConfPath = metricsConf.getAbsolutePath();

        extractPySparkLibrary(tempDir, extraPySparkFiles);
      } else {
        // Localize all user requested files in distributed mode
        distributedUserResources(context.getLocalizeResources(), localizeResources);

        // Localize program jar and the expanding program jar
        File programJar = Locations.linkOrCopy(runtimeContext.getProgram().getJarLocation(),
                                               new File(tempDir, SparkRuntimeContextProvider.PROGRAM_JAR_NAME));
        File expandedProgramJar = Locations.linkOrCopy(runtimeContext.getProgram().getJarLocation(),
                                                       new File(tempDir,
                                                                SparkRuntimeContextProvider.PROGRAM_JAR_EXPANDED_NAME));
        // Localize both the unexpanded and expanded program jar
        localizeResources.add(new LocalizeResource(programJar));
        localizeResources.add(new LocalizeResource(expandedProgramJar, true));

        if (pluginArchive != null) {
          localizeResources.add(new LocalizeResource(pluginArchive, true));
        }

        // Create and localize the launcher jar, which is for setting up services and classloader for spark containers
        localizeResources.add(new LocalizeResource(createLauncherJar(tempDir)));

        // Create metrics conf file in the current directory since
        // the same value for the "spark.metrics.conf" config needs to be used for both driver and executor processes
        // Also localize the metrics conf file to the executor nodes
        File metricsConf = SparkMetricsSink.writeConfig(new File(CDAP_METRICS_PROPERTIES));
        metricsConfPath = metricsConf.getName();
        localizeResources.add(new LocalizeResource(metricsConf));

        prepareHBaseDDLExecutorResources(tempDir, cConfCopy, localizeResources);

        // Localize the cConf file
        localizeResources.add(new LocalizeResource(saveCConf(cConfCopy, tempDir)));

        // Preserves and localize runtime information in the hConf
        Configuration hConf = contextConfig.set(runtimeContext, pluginArchive).getConfiguration();
        localizeResources.add(new LocalizeResource(saveHConf(hConf, tempDir)));

        // Joiner for creating classpath for spark containers
        Joiner joiner = Joiner.on(File.pathSeparator).skipNulls();

        // Localize the spark.jar archive, which contains all CDAP and dependency jars
        File sparkJar = new File(tempDir, CDAP_SPARK_JAR);
        classpath = joiner.join(Iterables.transform(buildDependencyJar(sparkJar), new Function<String, String>() {
          @Override
          public String apply(String name) {
            return Paths.get("$PWD", CDAP_SPARK_JAR, name).toString();
          }
        }));
        localizeResources.add(new LocalizeResource(sparkJar, true));

        // Localize logback if there is one. It is placed at the beginning of the classpath
        File logbackJar = ProgramRunners.createLogbackJar(new File(tempDir, "logback.xml.jar"));
        if (logbackJar != null) {
          localizeResources.add(new LocalizeResource(logbackJar));
          classpath = joiner.join(Paths.get("$PWD", logbackJar.getName()), classpath);
        }

        // Localize extra jars and append to the end of the classpath
        List<String> extraJars = new ArrayList<>();
        for (URI jarURI : CConfigurationUtil.getExtraJars(cConfCopy)) {
          extraJars.add(Paths.get("$PWD", LocalizationUtils.getLocalizedName(jarURI)).toString());
          localizeResources.add(new LocalizeResource(jarURI, false));
        }
        classpath = joiner.join(classpath, joiner.join(extraJars));
      }

      Iterable<URI> pyFiles = Collections.emptyList();
      if (context.isPySpark()) {
        extraPySparkFiles.add(PySparkUtil.createPySparkLib(tempDir));
        pyFiles = Iterables.concat(Iterables.transform(extraPySparkFiles, FILE_TO_URI),
                                   context.getAdditionalPythonLocations());
      }

      final Map<String, String> configs = createSubmitConfigs(tempDir, metricsConfPath, classpath,
                                                              context.getLocalizeResources(), isLocal, pyFiles);
      submitSpark = new Callable<ListenableFuture<RunId>>() {
        @Override
        public ListenableFuture<RunId> call() throws Exception {
          // If stop already requested on this service, don't submit the spark.
          // This happen when stop() was called whiling starting
          if (!isRunning()) {
            return immediateCancelledFuture();
          }
          return sparkSubmitter.submit(runtimeContext, configs, localizeResources, jobFile, runtimeContext.getRunId());
        }
      };
    } catch (Throwable t) {
      cleanupTask.run();
      if (t instanceof Error) {
        // Need to wrap Error. Otherwise, listeners of this Guava Service may not be called if the
        // initialization of the user program is missing dependencies (CDAP-2543).
        // Guava 15.0+ have this condition fixed, hence wrapping is no longer needed if upgrade to later Guava.
        throw new Exception(t);
      }
      throw t;
    }
  }

  @Override
  protected void run() throws Exception {
    ListenableFuture<RunId> jobCompletion = completion.getAndSet(submitSpark.call());
    // If the jobCompletion is not null, meaning the stop() was called before the atomic reference "completion" has been
    // updated. This mean the job is cancelled. We also need to cancel the future returned by submitSpark.call().
    if (jobCompletion != null) {
      completion.get().cancel(true);
    } else {
      // It's possible that the completion reference is changed by the triggerShutdown call between the getAndSet()
      // and the get() call here. But it's ok since the triggeredShutdown will always put a cancelled future in the
      // atomic reference and cancel the actual one.
      jobCompletion = completion.get();
    }

    try {
      // Block for job completion
      jobCompletion.get();
    } catch (Exception e) {
      // See if it is due to job cancelation. If it is, then it's not an error.
      if (jobCompletion.isCancelled()) {
        LOG.info("Spark program execution cancelled: {}", runtimeContext);
      } else {
        throw e;
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    // Try to get from the submission future to see if the job completed successfully.
    ListenableFuture<RunId> jobCompletion = completion.get();
    ProgramState state = new ProgramState(ProgramStatus.COMPLETED, null);
    try {
      jobCompletion.get();
    } catch (Exception e) {
      if (jobCompletion.isCancelled()) {
        state = new ProgramState(ProgramStatus.KILLED, null);
      } else {
        state = new ProgramState(ProgramStatus.FAILED, Throwables.getRootCause(e).getMessage());
      }
    }

    try {
      destroy(state);
    } finally {
      cleanupTask.run();
      LOG.debug("Spark program completed: {}", runtimeContext);
    }
  }

  @Override
  protected void triggerShutdown() {
    LOG.debug("Stop requested for Spark Program {}", runtimeContext);
    // Replace the completion future with a cancelled one.
    // Also, try to cancel the current completion future if it exists.
    ListenableFuture<RunId> future = completion.getAndSet(this.<RunId>immediateCancelledFuture());
    if (future != null) {
      future.cancel(true);
    }
  }

  @Override
  protected Executor executor() {
    // Always execute in new daemon thread.
    //noinspection NullableProblems
    return new Executor() {
      @Override
      public void execute(final Runnable runnable) {
        final Thread t = new Thread(new Runnable() {

          @Override
          public void run() {
            // note: this sets logging context on the thread level
            LoggingContextAccessor.setLoggingContext(runtimeContext.getLoggingContext());
            runnable.run();
          }
        });
        t.setDaemon(true);
        t.setName("SparkRunner" + runtimeContext.getProgramName());
        t.start();
      }
    };
  }

  /**
   * Calls the {@link Spark#beforeSubmit(SparkClientContext)} for the pre 3.5 Spark programs, calls
   * the {@link ProgramLifecycle#initialize} otherwise.
   */
  @SuppressWarnings("unchecked")
  private void initialize() throws Exception {
    context.setState(new ProgramState(ProgramStatus.INITIALIZING, null));

    // AbstractSpark implements final initialize(context) and requires subclass to
    // implement initialize(), whereas programs that directly implement Spark have
    // the option to override initialize(context) (if they implement ProgramLifeCycle)
    TransactionControl defaultTxControl = runtimeContext.getDefaultTxControl();
    final TransactionControl txControl = spark instanceof AbstractSpark
      ? Transactions.getTransactionControl(defaultTxControl, AbstractSpark.class, spark, "initialize")
      : spark instanceof ProgramLifecycle
      ? Transactions.getTransactionControl(defaultTxControl, Spark.class,
                                           spark, "initialize", SparkClientContext.class)
      : defaultTxControl;

    runtimeContext.initializeProgram(programLifecycle, txControl, false);;
  }

  /**
   * Calls the destroy or onFinish method of {@link ProgramLifecycle}.
   */
  private void destroy(final ProgramState state) {
    context.setState(state);

    TransactionControl defaultTxControl = runtimeContext.getDefaultTxControl();
    TransactionControl txControl = spark instanceof ProgramLifecycle
      ? Transactions.getTransactionControl(defaultTxControl, Spark.class, spark, "destroy")
      : defaultTxControl;

    runtimeContext.destroyProgram(programLifecycle, txControl, false);
    if (emitFieldLineage()) {
      try {
        // here we cannot call context.flushRecord() since the WorkflowNodeState will need to record and store
        // the lineage information
        FieldLineageInfo info = new FieldLineageInfo(runtimeContext.getFieldLineageOperations());
        fieldLineageWriter.write(runtimeContext.getProgramRunId(), info);
      } catch (Throwable t) {
        LOG.warn("Failed to emit the field lineage operations for Spark {}", runtimeContext.getProgramRunId(), t);
      }
    }
  }

  private boolean emitFieldLineage() {
    if (runtimeContext.getFieldLineageOperations().isEmpty()) {
      // no operations to emit
      return false;
    }

    ProgramStatus status = context.getState().getStatus();
    if (ProgramStatus.COMPLETED != status) {
      // Spark program failed, so field operations wont be emitted
      return false;
    }

    WorkflowProgramInfo workflowInfo = runtimeContext.getWorkflowInfo();
    return workflowInfo == null || !workflowInfo.fieldLineageConsolidationEnabled();
  }

  /**
   * Creates a JAR file which contains generate Spark YARN container main classes. Those classes
   * are used for intercepting the Java main method in the YARN container so that we can control the
   * ClassLoader creation.
   */
  private File createLauncherJar(File tempDir) throws IOException {
    File jarFile = new File(tempDir, CDAP_LAUNCHER_JAR);
    ContainerLauncherGenerator.generateLauncherJar(
      Arrays.asList("org.apache.spark.deploy.yarn.ApplicationMaster",
                    "org.apache.spark.executor.CoarseGrainedExecutorBackend",
                    "org.apache.spark.executor.YarnCoarseGrainedExecutorBackend"),
      SparkContainerLauncher.class, jarFile);
    return jarFile;
  }

  /**
   * Creates the configurations for the spark submitter.
   */
  private Map<String, String> createSubmitConfigs(File localDir, String metricsConfPath, String classpath,
                                                  Map<String, LocalizeResource> localizedResources,
                                                  boolean localMode,
                                                  Iterable<URI> pyFiles) throws Exception {

    // Setup configs from the default spark conf
    Map<String, String> configs = new HashMap<>(Maps.fromProperties(SparkPackageUtils.getSparkDefaultConf()));

    // Make Spark UI runs on random port. By default, Spark UI runs on port 4040 and it will do a sequential search
    // of the next port if 4040 is already occupied. However, during the process, it unnecessarily logs big stacktrace
    // as WARN, which pollute the logs a lot if there are concurrent Spark job running (e.g. a fork in Workflow).
    // Spark k8s fails if you set port to 0...
    //configs.put("spark.ui.port", "0");

    // Setup app.id and executor.id for Metric System
    configs.put("spark.app.id", context.getApplicationSpecification().getName());
    configs.put("spark.executor.id", context.getApplicationSpecification().getName());

    // Setups the resources requirements for driver and executor. The user can override it with the SparkConf.
    configs.put("spark.driver.memory", context.getDriverResources().getMemoryMB() + "m");
    configs.put("spark.driver.cores", String.valueOf(context.getDriverResources().getVirtualCores()));
    configs.put("spark.executor.memory", context.getExecutorResources().getMemoryMB() + "m");
    configs.put("spark.executor.cores", String.valueOf(context.getExecutorResources().getVirtualCores()));

    // Setup the memory overhead.
    setMemoryOverhead(context.getDriverRuntimeArguments(), "driver",
                      context.getDriverResources().getMemoryMB(), configs);
    setMemoryOverhead(context.getExecutorRuntimeArguments(), "executor",
                      context.getExecutorResources().getMemoryMB(), configs);

    // Add user specified configs first. CDAP specifics config will override them later if there are duplicates.
    SparkConf sparkConf = context.getSparkConf();
    if (sparkConf != null) {
      for (Tuple2<String, String> tuple : sparkConf.getAll()) {
        configs.put(tuple._1(), tuple._2());
      }
    }

    //Configuration options for CDAP
    String sparkCheckpointTempRewrite =
      String.format("-D%s=%s", SparkRuntimeUtils.STREAMING_CHECKPOINT_REWRITE_ENABLED,
                    cConf.get(SparkRuntimeUtils.SPARK_STREAMING_CHECKPOINT_REWRITE_ENABLED));
    prependConfig(configs, "spark.driver.extraJavaOptions", sparkCheckpointTempRewrite, " ");
    prependConfig(configs, "spark.executor.extraJavaOptions", sparkCheckpointTempRewrite, " ");

    // CDAP-5854: On Windows * is a reserved character which cannot be used in paths. So adding the below to
    // classpaths will fail. Please see CDAP-5854.
    // In local mode spark program runs under the same jvm as cdap master and these jars will already be in the
    // classpath so adding them is not required. In non-local mode where spark driver and executors runs in a different
    // jvm we are adding these to their classpath.
    if (!localMode) {
      String extraClassPath = Paths.get("$PWD", CDAP_LAUNCHER_JAR) + File.pathSeparator + classpath;

      // Set extraClasspath config by appending user specified extra classpath
      prependConfig(configs, "spark.driver.extraClassPath", extraClassPath, File.pathSeparator);
      prependConfig(configs, "spark.executor.extraClassPath", extraClassPath, File.pathSeparator);

      // Prepend the extra java opts
      prependConfig(configs, "spark.driver.extraJavaOptions", cConf.get(Constants.AppFabric.PROGRAM_JVM_OPTS), " ");
      prependConfig(configs, "spark.executor.extraJavaOptions", cConf.get(Constants.AppFabric.PROGRAM_JVM_OPTS), " ");
    } else {
      // Only need to set this for local mode.
      // In distributed mode, Spark will not use this but instead use the yarn container directory.
      configs.put("spark.local.dir", localDir.getAbsolutePath());
    }

    // Add the spark listener for CDAP
    prependConfig(configs, "spark.extraListeners", DelegatingSparkListener.class.getName(), ",");

    configs.put("spark.metrics.conf", metricsConfPath);
    SparkRuntimeUtils.setLocalizedResources(localizedResources.keySet(), configs);

    if (context.isPySpark()) {
      Iterable<String> pyFilePaths = Iterables.transform(pyFiles, new Function<URI, String>() {
        @Override
        public String apply(URI input) {
          if (input.getScheme() == null || "file".equals(input.getScheme())) {
            return input.getPath();
          }
          return input.toString();
        }
      });
      String pyFilesConfig = Joiner.on(",").join(pyFilePaths);
      configs.put("spark.submit.pyFiles", pyFilesConfig);

      // In local mode, since SPARK_HOME is not set, we need to set this property such that it will get pickup
      // in PythonWorkerFactory (via SparkClassRewriter) so that the pyspark library is available.
      if (isLocal) {
        SparkRuntimeEnv.setProperty("cdap.spark.pyFiles", Joiner.on(File.pathSeparator).join(pyFilePaths));
      }
    }

    return configs;
  }

  /**
   * Sets the Spark memory overhead setting based on the runtime arguments of the given type.
   *
   * @param runtimeArgs the runtime arguments
   * @param type either {@code driver} or {@code executor}
   * @param containerSize the memory size in MB of the container
   * @param configs the configuration to be updated
   */
  private void setMemoryOverhead(Map<String, String> runtimeArgs, String type,
                                 int containerSize, Map<String, String> configs) {
    Map<String, String> memoryConfigs = SystemArguments.getTwillContainerConfigs(runtimeArgs, containerSize);
    if (!memoryConfigs.containsKey(Configs.Keys.JAVA_RESERVED_MEMORY_MB)) {
      return;
    }
    configs.put("spark.yarn." + type + ".memoryOverhead", memoryConfigs.get(Configs.Keys.JAVA_RESERVED_MEMORY_MB));
  }

  /**
   * Sets a key value pair to the given configuration map. If the key already exist, the given value will be
   * prepended to the existing value.
   */
  private void prependConfig(Map<String, String> configs, String key, String prepend, String separator) {
    String existing = configs.get(key);
    if (existing == null) {
      configs.put(key, prepend);
    } else {
      configs.put(key, prepend + separator + existing);
    }
  }

  /**
   * Packages all the dependencies of the Spark job. It contains all CDAP classes that are needed to run the
   * user spark program.
   *
   * @param targetFile the target file for the jar created
   * @return list of jar file name that contains all dependency jars in sorted order
   * @throws IOException if failed to package the jar
   */
  private Iterable<String> buildDependencyJar(File targetFile) throws IOException, URISyntaxException {
    Set<String> classpath = new TreeSet<>();
    try (JarOutputStream jarOut = new JarOutputStream(new BufferedOutputStream(new FileOutputStream(targetFile)))) {
      jarOut.setLevel(Deflater.NO_COMPRESSION);

      // Zip all the jar files under the same directory that contains the jar for this class and twill class.
      // Those are the directory created by TWILL that contains all dependency jars for this container
      for (String className : Arrays.asList(getClass().getName(), TwillRunnable.class.getName())) {
        Enumeration<URL> resources = getClass().getClassLoader().getResources(className.replace('.', '/') + ".class");
        while (resources.hasMoreElements()) {
          URL classURL = resources.nextElement();
          File libDir = new File(ClassLoaders.getClassPathURL(className, classURL).toURI()).getParentFile();

          for (File file : DirUtils.listFiles(libDir, "jar")) {
            if (classpath.add(file.getName())) {
              jarOut.putNextEntry(new JarEntry(file.getName()));
              Files.copy(file, jarOut);
              jarOut.closeEntry();
            }
          }
        }
      }
    }
    return classpath;
  }

  /**
   * Extracts pyspark and py4j libraries into the given temporary directory. This method is only used in local mode.
   *
   * @param tempDir temporary directory for storing the file
   * @param result the collection for storing the result
   */
  private void extractPySparkLibrary(File tempDir, Collection<File> result) throws IOException {
    URL py4jURL = getClass().getClassLoader().getResource("pyspark/py4j-src.zip");
    if (py4jURL == null) {
      // This shouldn't happen
      throw new IOException("Failed to locate py4j-src.zip, which is required to run PySpark");
    }
    File file = new File(tempDir, "py4j-src.zip");
    try (FileOutputStream fos = new FileOutputStream(file)) {
      Resources.copy(py4jURL, fos);
    }
    result.add(file);

    URL pysparkURL = getClass().getClassLoader().getResource("pyspark/pyspark.zip");
    if (pysparkURL == null) {
      // This shouldn't happen
      throw new IOException("Failed to locate pyspark.zip, which is required to run PySpark");
    }

    file = new File(tempDir, "pyspark.zip");
    try (FileOutputStream fos = new FileOutputStream(file)) {
      Resources.copy(pysparkURL, fos);
    }
    result.add(file);
  }

  /**
   * Gets the {@link URI} for the Python script for submitting to PySpark.
   *
   * @param tempDir a temporary directory for copying the python script if necessary.
   * @return the {@link URI} for submitting to PySpark.
   * @throws IOException if failed to retrieve the Python script.
   */
  private URI getPySparkScript(File tempDir) throws IOException, URISyntaxException {
    // This shouldn't happen, caller should already checked
    Preconditions.checkState(context.isPySpark());

    ProgramRunId programRunId = runtimeContext.getProgramRunId();
    File pythonFile = new File(tempDir, String.format("%s.%s.%s.%s.py",
                                                      programRunId.getNamespace(),
                                                      programRunId.getApplication(),
                                                      programRunId.getProgram(),
                                                      programRunId.getRun()));
    if (context.getPySparkScript() != null) {
      Files.write(context.getPySparkScript(), pythonFile, StandardCharsets.UTF_8);
      return pythonFile.toURI();
    }

    URI scriptURI = context.getPySparkScriptLocation();
    if (scriptURI == null) {
      // This shouldn't happen since context.isPySpark guarantees either the script or the location is not null
      throw new IllegalStateException("Missing Python script to run PySpark");
    }

    URI homeURI = locationFactory.getHomeLocation().toURI();

    // If no scheme, default it to the location factory.
    if (scriptURI.getScheme() == null) {
      scriptURI = new URI(homeURI.getScheme(), homeURI.getAuthority(), scriptURI.getPath(), scriptURI.getFragment());
    }

    // See if the scriptURI is on the same cluster.
    if (Objects.equals(homeURI.getScheme(), scriptURI.getScheme())
      && Objects.equals(homeURI.getAuthority(), scriptURI.getAuthority())) {
      // If the extension is ".py", just return it.
      if (scriptURI.getPath().endsWith(".py")) {
        return scriptURI;
      }
      return Locations.linkOrCopy(locationFactory.create(scriptURI), pythonFile).toURI();
    }

    // Local file
    if ("file".equals(scriptURI.getScheme())) {
      // If the extension is ".py", just return it.
      if (scriptURI.getPath().endsWith(".py")) {
        return scriptURI;
      }
      // Otherwise, need to link/copy it.
      return Locations.linkOrCopy(Locations.toLocation(new File(scriptURI.getPath())), pythonFile).toURI();
    }

    try {
      // The URI is from some unknown scheme. Try to use FileSystem to copy the script.
      Configuration hConf = runtimeContext.getConfiguration();
      hConf.set(String.format("fs.%s.impl.disable.cache", scriptURI.getScheme()), "true");
      try (
        FileSystem fs = FileSystem.get(scriptURI, hConf);
        InputStream is = fs.open(new Path(scriptURI))
      ) {
        ByteStreams.copy(is, Files.newOutputStreamSupplier(pythonFile));
        return pythonFile.toURI();
      }
    } catch (IOException e) {
      // Not able to copy it with FileSystem. Just log a debug as we'll try to open the URL as last resort
      LOG.debug("Failed to copy python script from uri {}.", scriptURI, e);
    }

    // Last resort, turn the URI to URL and try to copy from it.
    try (InputStream is = scriptURI.toURL().openStream()) {
      ByteStreams.copy(is, Files.newOutputStreamSupplier(pythonFile));
    }
    return pythonFile.toURI();
  }

  /**
   * Generates the job jar, which is always be an empty jar file
   *
   * @return The generated {@link File} in the given target directory
   */
  private URI createJobJar(File tempDir) throws IOException {
    // In local mode, Spark Streaming with checkpointing will expect the same job jar to exist
    // in all runs of the program. This means it can't be created in the temporary directory for the run,
    // but must persist between runs
    File targetDir = isLocal
      ? new File(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR), "runtime"), "spark")
      : tempDir;

    DirUtils.mkdirs(targetDir.getAbsoluteFile());
    File tempFile = new File(targetDir, "cdapSparkJob.jar");
    if (tempFile.exists()) {
      return tempFile.toURI();
    }

    try (JarOutputStream output = new JarOutputStream(new FileOutputStream(tempFile))) {
      return tempFile.toURI();
    }
  }

  /**
   * Serialize {@link CConfiguration} to a file.
   *
   * @return The {@link File} of the serialized configuration in the given target directory.
   */
  private File saveCConf(CConfiguration cConf, File targetDir) throws IOException {
    File file = new File(targetDir, SparkRuntimeContextProvider.CCONF_FILE_NAME);
    try (Writer writer = Files.newWriter(file, Charsets.UTF_8)) {
      cConf.writeXml(writer);
    }
    return file;
  }

  /**
   * Serialize {@link Configuration} to a file.
   *
   * @return The {@link File} of the serialized configuration in the given target directory.
   */
  private File saveHConf(Configuration hConf, File targetDir) throws IOException {
    File file = new File(targetDir, SparkRuntimeContextProvider.HCONF_FILE_NAME);
    try (Writer writer = Files.newWriter(file, Charsets.UTF_8)) {
      hConf.writeXml(writer);
    }
    return file;
  }

  /**
   * Localizes resources to the given local directory. For resources on the local directory already, a symlink will be
   * created. Otherwise, it will be copied.
   *
   * @param resources the set of resources that need to be localized.
   * @param targetDir the target directory for the resources to copy / link to.
   */
  private void copyUserResources(Map<String, LocalizeResource> resources, File targetDir) throws IOException {
    for (Map.Entry<String, LocalizeResource> entry : resources.entrySet()) {
      LocalizationUtils.localizeResource(entry.getKey(), entry.getValue(), targetDir);
    }
  }

  /**
   * Appends user localize resources to the given {@link List} of {@link LocalizeResource}.
   */
  private void distributedUserResources(Map<String, LocalizeResource> resources,
                                        List<LocalizeResource> result) throws URISyntaxException {
    for (Map.Entry<String, LocalizeResource> entry : resources.entrySet()) {
      URI uri = entry.getValue().getURI();
      URI actualURI = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), uri.getQuery(), entry.getKey());
      result.add(new LocalizeResource(actualURI, entry.getValue().isArchive()));
    }
  }

  /**
   * Creates a {@link Runnable} to be executed to cleanup resources after executing the spark program.
   *
   * @param directory The directory to be deleted
   * @param properties The set of system {@link Properties} prior to the job execution.
   */
  private Runnable createCleanupTask(final File directory, Properties properties) {
    final Map<String, String> retainingProperties = new HashMap<>();
    for (String key : properties.stringPropertyNames()) {
      if (key.startsWith("spark.")) {
        retainingProperties.put(key, properties.getProperty(key));
      }
    }

    return new Runnable() {

      @Override
      public void run() {
        cleanupShutdownHooks();
        invalidateBeanIntrospectorCache();

        // Cleanup all system properties setup by SparkSubmit
        Iterable<String> sparkKeys = Iterables.filter(System.getProperties().stringPropertyNames(),
                                                      Predicates.containsPattern("^spark\\."));
        for (String key : sparkKeys) {
          if (retainingProperties.containsKey(key)) {
            String value = retainingProperties.get(key);
            LOG.debug("Restoring Spark system property: {} -> {}", key, value);
            System.setProperty(key, value);
          } else {
            LOG.debug("Removing Spark system property: {}", key);
            System.clearProperty(key);
          }
        }

        try {
          DirUtils.deleteDirectoryContents(directory);
        } catch (IOException e) {
          LOG.warn("Failed to cleanup directory {}", directory);
        }
      }
    };
  }

  /**
   * Cleanup all shutdown hooks added by Spark and execute them directly.
   * This is needed so that for CDAP standalone, it won't leak memory through shutdown hooks.
   */
  private void cleanupShutdownHooks() {
    // With Hadoop 2, Spark uses the Hadoop ShutdownHookManager
    ShutdownHookManager manager = ShutdownHookManager.get();
    try {
      // Use reflection to get the shutdown hooks
      Method getShutdownHooksInOrder = manager.getClass().getDeclaredMethod("getShutdownHooksInOrder");
      if (!Collection.class.isAssignableFrom(getShutdownHooksInOrder.getReturnType())) {
        LOG.warn("Unsupported method {}. Spark shutdown hooks cleanup skipped.", getShutdownHooksInOrder);
        return;
      }
      getShutdownHooksInOrder.setAccessible(true);

      // Filter out hooks that are defined in the same SparkRunnerClassLoader as this SparkProgramRunner class
      // This is for the case when there are concurrent Spark job running in the same VM
      for (Object hookEntry : (Collection<?>) getShutdownHooksInOrder.invoke(manager)) {
        Runnable runnable = getShutdownHookRunnable(hookEntry);
        if (runnable != null && runnable.getClass().getClassLoader() == getClass().getClassLoader()) {
          LOG.debug("Running Spark shutdown hook {}", runnable);
          runnable.run();
          manager.removeShutdownHook(runnable);
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to cleanup Spark shutdown hooks.", e);
    }
  }

  @Nullable
  private Runnable getShutdownHookRunnable(Object hookEntry) {
    try {
      // Pre Hadoop-2.8, the entry is the Runnable added to the ShutdownHookManager
      // Post Hadoop-2.8, the entry is the ShutdownHookManager.HookEntry class
      if (!(hookEntry instanceof Runnable)) {
        Field hookField = hookEntry.getClass().getDeclaredField("hook");
        hookField.setAccessible(true);
        hookEntry = hookField.get(hookEntry);
      }

      if (hookEntry instanceof Runnable) {
        return (Runnable) hookEntry;
      }

      throw new UnsupportedOperationException("Hook entry is not a Runnable: " + hookEntry.getClass().getName());
    } catch (Exception e) {
      LOG.warn("Failed to get Spark shutdown hook Runnable from Hadoop ShutdownHookManager hook entry {} due to {}",
               hookEntry, e.toString());
    }
    return null;
  }

  /**
   * Clear the constructor cache in the BeanIntrospector to avoid leaking ClassLoader.
   */
  private void invalidateBeanIntrospectorCache() {
    try {
      // Get the class through reflection since some Spark version doesn't depend on the fasterxml scala module.
      // This is to avoid class not found issue
      Class<?> cls = Class.forName("com.fasterxml.jackson.module.scala.introspect.BeanIntrospector$");
      Field field = Fields.findField(cls, "ctorParamNamesCache");

      // See if it is a LoadingCache. Need to check with class name since in distributed mode assembly jar, the
      // guava classes are shaded and renamed.
      switch (field.getType().getName()) {
        // Get the cache field and invalid it.
        // The BeanIntrospector is a scala object and scala generates a static MODULE$ field to the singleton object
        // We need to make both two different class name because in the Spark assembly jar, the guava classes
        // get renamed. In unit-test, however, the class won't get renamed because they are pulled from dependency
        case "com.google.common.cache.LoadingCache":
        case "org.spark-project.guava.cache.LoadingCache":
          field.setAccessible(true);
          Object cache = field.get(Fields.findField(cls, "MODULE$").get(null));
          // Try to call the invalidateAll method of the cache
          Method invalidateAll = cache.getClass().getMethod("invalidateAll");
          invalidateAll.setAccessible(true);
          invalidateAll.invoke(cache);
          LOG.debug("BeanIntrospector.ctorParamNamesCache has been invalidated.");
          break;

        default:
          // Unexpected, maybe due to version change in the BeanIntrospector, hence log a WARN.
          LOG.warn("BeanIntrospector.ctorParamNamesCache is not a LoadingCache, may lead to memory leak in SDK." +
                     "Field type is {}", field.getType());
      }
    } catch (NoSuchFieldException e) {
      // If there is no ctorParamNamesCache field, there is nothing to invalidate.
      // This is the case in jackson-module-scala_2.11-2.6.5 used by Spark 2.1.0
      LOG.trace("No ctorParamNamesCache field in BeanIntrospector. " +
                  "The current Spark version is not using a BeanIntrospector that has a param names loading cache.");
    } catch (ClassNotFoundException e) {
      // Catch the case when there is no BeanIntrospector class. It is ok since some Spark version may not be using it.
      LOG.debug("No BeanIntrospector class found. The current Spark version is not using BeanIntrospector.");
    } catch (Exception e) {
      LOG.warn("Failed to cleanup BeanIntrospector cache, may lead to memory leak in SDK.", e);
    }
  }

  /**
   * Creates a {@link ListenableFuture} that is cancelled.
   */
  private <V> ListenableFuture<V> immediateCancelledFuture() {
    SettableFuture<V> future = SettableFuture.create();
    future.cancel(true);
    return future;
  }

  /**
   * Prepares the {@link HBaseDDLExecutor} implementation for localization.
   */
  private void prepareHBaseDDLExecutorResources(File tempDir, CConfiguration cConf,
                                                List<LocalizeResource> localizeResources)
    throws IOException {
    String ddlExecutorExtensionDir = cConf.get(Constants.HBaseDDLExecutor.EXTENSIONS_DIR);
    if (ddlExecutorExtensionDir == null) {
      // Nothing to localize
      return;
    }

    final File target = new File(tempDir, "hbaseddlext.jar");
    BundleJarUtil.createJar(new File(ddlExecutorExtensionDir), target);
    localizeResources.add(new LocalizeResource(target, true));
    cConf.set(Constants.HBaseDDLExecutor.EXTENSIONS_DIR, target.getName());
  }
}
