/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.provision;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.async.KeyedExecutor;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.plugin.MacroParser;
import io.cdap.cdap.internal.app.spark.SparkCompatReader;
import io.cdap.cdap.internal.pipeline.PluginRequirement;
import io.cdap.cdap.internal.provision.task.DeprovisionTask;
import io.cdap.cdap.internal.provision.task.ProvisionTask;
import io.cdap.cdap.internal.provision.task.ProvisioningTask;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.provisioner.ProvisionerDetail;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.runtime.spi.SparkCompat;
import io.cdap.cdap.runtime.spi.VersionInfo;
import io.cdap.cdap.runtime.spi.provisioner.Capabilities;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSystemContext;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import io.cdap.cdap.runtime.spi.ssh.SSHContext;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Service for provisioning related operations
 */
public class ProvisioningService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(ProvisioningService.class);
  private static final Logger SAMPLING_LOG = Loggers.sampling(LOG, LogSamplers.onceEvery(20));
  private static final Gson GSON = new Gson();
  private static final Type PLUGIN_REQUIREMENT_SET_TYPE = new TypeToken<Set<PluginRequirement>>() { }.getType();

  private final CConfiguration cConf;
  private final AtomicReference<ProvisionerInfo> provisionerInfo;
  private final ProvisionerProvider provisionerProvider;
  private final ProvisionerConfigProvider provisionerConfigProvider;
  private final ProvisionerNotifier provisionerNotifier;
  private final LocationFactory locationFactory;
  private final SparkCompat sparkCompat;
  private final SecureStore secureStore;
  private final Consumer<ProgramRunId> taskStateCleanup;
  private final ProgramStateWriter programStateWriter;
  private final ProvisionerStore provisionerStore;
  private final TransactionRunner transactionRunner;
  private final MetricsCollectionService metricsCollectionService;
  private KeyedExecutor<ProvisioningTaskKey> taskExecutor;
  private ExecutorService contextExecutor;

  @Inject
  ProvisioningService(CConfiguration cConf, ProvisionerProvider provisionerProvider,
                      ProvisionerConfigProvider provisionerConfigProvider,
                      ProvisionerNotifier provisionerNotifier, LocationFactory locationFactory,
                      SecureStore secureStore, ProgramStateWriter programStateWriter,
                      ProvisionerStore provisionerStore, TransactionRunner transactionRunner,
                      MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    this.provisionerProvider = provisionerProvider;
    this.provisionerConfigProvider = provisionerConfigProvider;
    this.provisionerNotifier = provisionerNotifier;
    this.provisionerInfo = new AtomicReference<>(new ProvisionerInfo(new HashMap<>(), new HashMap<>()));
    this.locationFactory = locationFactory;
    this.sparkCompat = SparkCompatReader.get(cConf);
    this.secureStore = secureStore;
    this.programStateWriter = programStateWriter;
    this.provisionerStore = provisionerStore;
    this.transactionRunner = transactionRunner;
    this.metricsCollectionService = metricsCollectionService;
    this.taskStateCleanup = programRunId -> {
      try {
        provisionerStore.deleteTaskInfo(programRunId);
      } catch (IOException e) {
        LOG.error("Error cleaning up provisioning task {}", programRunId.toString(), e);
      }
    };
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}", getClass().getSimpleName());

    this.taskExecutor = new KeyedExecutor<>(
      Executors.newScheduledThreadPool(cConf.getInt(Constants.Provisioner.EXECUTOR_THREADS),
                                       Threads.createDaemonThreadFactory("provisioning-task-%d")));

    int maxPoolSize = cConf.getInt(Constants.Provisioner.CONTEXT_EXECUTOR_THREADS);
    ThreadPoolExecutor contextExecutor = new ThreadPoolExecutor(
      maxPoolSize, maxPoolSize, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
      Threads.createDaemonThreadFactory("provisioning-context-%d"));
    contextExecutor.allowCoreThreadTimeOut(true);
    this.contextExecutor = contextExecutor;

    initializeProvisioners();
    resumeTasks(taskStateCleanup);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}", getClass().getSimpleName());
    try {
      // Shutdown the executor, which will issue an interrupt to the running thread.
      // Wait for a moment for threads to complete. Even if they don't, however, it also ok since we have
      // the state persisted and the threads are daemon threads.
      taskExecutor.shutdownNow();
      taskExecutor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      // Ignore it.
    }

    contextExecutor.shutdownNow();
    LOG.info("Stopped {}", getClass().getSimpleName());
  }

  /**
   * Returns the {@link ClusterStatus} for the cluster being used to execute the given program run.
   *
   * @param programRunId the program run id for checking the cluster status
   * @param programOptions the program options for the given run
   * @param cluster the {@link Cluster} information for the given run
   * @param userId the user id to use for {@link SecureStore} operation.
   * @return the {@link ClusterStatus}
   * @throws Exception if non-retryable exception is encountered when querying cluster status
   */
  public ClusterStatus getClusterStatus(ProgramRunId programRunId, ProgramOptions programOptions,
                                        Cluster cluster, String userId) throws Exception {
    Map<String, String> systemArgs = programOptions.getArguments().asMap();
    String name = SystemArguments.getProfileProvisioner(systemArgs);
    Provisioner provisioner = provisionerInfo.get().provisioners.get(name);

    // If there is no provisioner available, we can't do anything further, hence returning NOT_EXISTS
    if (provisioner == null) {
      return ClusterStatus.NOT_EXISTS;
    }

    Map<String, String> properties = SystemArguments.getProfileProperties(systemArgs);

    // Create the ProvisionerContext and query the cluster status using the provisioner
    ProvisionerContext context;
    try {
      DefaultSSHContext defaultSSHContext = null;
      if (!getRuntimeJobManager(programRunId, programOptions).isPresent()) {
        defaultSSHContext = new DefaultSSHContext(
          Networks.getAddress(cConf, Constants.NETWORK_PROXY_ADDRESS), null, null);
      }
      context = createContext(cConf, programOptions, programRunId, userId, properties, defaultSSHContext);
    } catch (InvalidMacroException e) {
      // This shouldn't happen
      runWithProgramLogging(programRunId, systemArgs,
                            () -> LOG.error("Could not evaluate macros while checking cluster status.", e));
      return ClusterStatus.NOT_EXISTS;
    }

    return Retries.callWithRetries(() -> provisioner.getClusterStatus(context, cluster),
                                   RetryStrategies.exponentialDelay(1, 5, TimeUnit.SECONDS),
                                   RetryableProvisionException.class::isInstance);
  }

  @Nullable
  private VersionInfo getAppCDAPVersion(ProgramOptions programOptions) {
    String version = programOptions.getArguments().getOption(Constants.APP_CDAP_VERSION);
    return version == null ? null : new ProjectInfo.Version(version);
  }

  /**
   * Scans the ProvisionerStore for any tasks that should be in progress but are not being executed and consumes
   * them.
   */
  @VisibleForTesting
  void resumeTasks(Consumer<ProgramRunId> taskCleanup) throws IOException {
    // TODO: CDAP-13462 read tasks in chunks to avoid tx timeout
    List<ProvisioningTaskInfo> clusterTaskInfos = getInProgressTasks();

    for (ProvisioningTaskInfo provisioningTaskInfo : clusterTaskInfos) {
      State serviceState = state();
      // normally this is only called when the service is starting, but it can be running in unit test
      if (serviceState != State.STARTING && serviceState != State.RUNNING) {
        return;
      }
      ProvisioningTaskKey taskKey = provisioningTaskInfo.getTaskKey();
      Optional<Future<Void>> taskFuture = taskExecutor.getFuture(taskKey);
      if (taskFuture.isPresent()) {
        // don't resume the task if it's already been submitted.
        // this should only happen if a program run is started before we scanned for tasks
        continue;
      }
      ProgramRunId programRunId = provisioningTaskInfo.getProgramRunId();

      ProvisioningOp provisioningOp = provisioningTaskInfo.getProvisioningOp();
      String provisionerName = provisioningTaskInfo.getProvisionerName();
      Provisioner provisioner = provisionerInfo.get().provisioners.get(provisionerName);

      if (provisioner == null) {
        // can happen if CDAP is shut down in the middle of a task, and a provisioner is removed
        LOG.error("Could not provision cluster for program run {} because provisioner {} no longer exists.",
                  programRunId, provisionerName);
        provisionerNotifier.orphaned(programRunId);
        provisionerStore.deleteTaskInfo(provisioningTaskInfo.getProgramRunId());
        continue;
      }

      Runnable task;
      switch (provisioningOp.getType()) {
        case PROVISION:
          task = createProvisionTask(provisioningTaskInfo, provisioner);
          break;
        case DEPROVISION:
          task = createDeprovisionTask(provisioningTaskInfo, provisioner, taskCleanup);
          break;
        default:
          LOG.error("Skipping unknown provisioning task type {}", provisioningOp.getType());
          continue;
      }

      // the actual task still needs to be run for cleanup to happen, but avoid logging a confusing
      // message about resuming a task that is in cancelled state or resuming a task that is completed
      if (provisioningOp.getStatus() != ProvisioningOp.Status.CANCELLED &&
        provisioningOp.getStatus() != ProvisioningOp.Status.CREATED) {
        LOG.info("Resuming provisioning task for run {} of type {} in state {}.",
                 provisioningTaskInfo.getProgramRunId(), provisioningTaskInfo.getProvisioningOp().getType(),
                 provisioningTaskInfo.getProvisioningOp().getStatus());
      }
      task.run();
    }
  }

  /**
   * Cancel the provision task for the program run. If there is no task or the task could not be cancelled before it
   * completed, an empty optional will be returned. If the task was cancelled, the state of the cancelled task will
   * be returned, and a message will be sent to mark the program run state as killed.
   *
   * @param programRunId the program run
   * @return the state of the task if it was cancelled
   */
  public Optional<ProvisioningTaskInfo> cancelProvisionTask(ProgramRunId programRunId) throws IOException {
    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(programRunId, ProvisioningOp.Type.PROVISION);
    return cancelTask(taskKey, programStateWriter::killed);
  }

  /**
   * Cancel the deprovision task for the program run. If there is no task or the task could not be cancelled before it
   * completed, an empty optional will be returned. If the task was cancelled, the state of the cancelled task will
   * be returned, and a message will be sent to mark the program run as orphaned.
   *
   * @param programRunId the program run
   * @return the state of the task if it was cancelled
   */
  Optional<ProvisioningTaskInfo> cancelDeprovisionTask(ProgramRunId programRunId) throws IOException {
    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(programRunId, ProvisioningOp.Type.DEPROVISION);
    return cancelTask(taskKey, provisionerNotifier::orphaned);
  }

  /**
   * Record that a cluster will be provisioned for a program run, returning a Runnable that will actually perform
   * the cluster provisioning. This method must be run within a transaction.
   * The task returned should only be executed after the transaction that ran this method has completed.
   * Running the returned Runnable will start the actual task using an executor within this service so that it can be
   * tracked and optionally cancelled using {@link #cancelProvisionTask(ProgramRunId)}. The caller does not need to
   * submit the runnable using their own executor.
   *
   * @param provisionRequest the provision request
   * @param context context for the transaction
   * @return runnable that will actually execute the cluster provisioning
   */
  public Runnable provision(ProvisionRequest provisionRequest, StructuredTableContext context) throws IOException {
    ProgramRunId programRunId = provisionRequest.getProgramRunId();
    ProgramOptions programOptions = provisionRequest.getProgramOptions();
    Map<String, String> args = programOptions.getArguments().asMap();
    String name = SystemArguments.getProfileProvisioner(args);
    Provisioner provisioner = provisionerInfo.get().provisioners.get(name);
    // any errors seen here will transition the state straight to deprovisioned since no cluster create was attempted
    if (provisioner == null) {
      runWithProgramLogging(
        programRunId, args,
        () -> LOG.error("Could not provision cluster for the run because provisioner {} does not exist.", name));
      programStateWriter.error(programRunId, new IllegalStateException("Provisioner does not exist."));
      provisionerNotifier.deprovisioned(programRunId);
      return () -> { };
    }

    // get plugin requirement information and check for capability to run on the provisioner
    Set<PluginRequirement> requirements = GSON.fromJson(args.get(ProgramOptionConstants.PLUGIN_REQUIREMENTS),
                                                        PLUGIN_REQUIREMENT_SET_TYPE);
    if (requirements != null) {
      Set<PluginRequirement> unfulfilledRequirements =
        getUnfulfilledRequirements(provisioner.getCapabilities(), requirements);
      if (!unfulfilledRequirements.isEmpty()) {
        runWithProgramLogging(programRunId, args, () ->
          LOG.error(String.format("'%s' cannot be run using profile '%s' because the profile does not met all " +
                                    "plugin requirements. Following requirements were not meet by the listed " +
                                    "plugins: '%s'", programRunId.getProgram(), name,
                                  groupByRequirement(unfulfilledRequirements))));
        programStateWriter.error(programRunId, new IllegalArgumentException("Provisioner does not meet all the " +
                                                                              "requirements for the program to run."));
        provisionerNotifier.deprovisioned(programRunId);
        return () -> { };
      }
    }

    Map<String, String> properties = SystemArguments.getProfileProperties(args);
    ProvisioningOp provisioningOp = new ProvisioningOp(ProvisioningOp.Type.PROVISION,
                                                       ProvisioningOp.Status.REQUESTING_CREATE);
    ProvisioningTaskInfo provisioningTaskInfo =
      new ProvisioningTaskInfo(programRunId, provisionRequest.getProgramDescriptor(), programOptions,
                               properties, name, provisionRequest.getUser(), provisioningOp,
                               createKeysDirectory(programRunId).toURI(), null);
    ProvisionerTable provisionerTable = new ProvisionerTable(context);
    provisionerTable.putTaskInfo(provisioningTaskInfo);
    return createProvisionTask(provisioningTaskInfo, provisioner);
  }

  /**
   * Returns runtime job manager implementation.
   *
   * @param programRunId program run
   * @param programOptions program options
   * @return an object of runtime job manager
   */
  public Optional<RuntimeJobManager> getRuntimeJobManager(ProgramRunId programRunId, ProgramOptions programOptions) {
    Map<String, String> systemArgs = programOptions.getArguments().asMap();
    String name = SystemArguments.getProfileProvisioner(systemArgs);
    Provisioner provisioner = provisionerInfo.get().provisioners.get(name);
    String user = programOptions.getArguments().getOption(ProgramOptionConstants.USER_ID);
    Map<String, String> properties = SystemArguments.getProfileProperties(systemArgs);
    ProvisionerContext context = createContext(cConf, programOptions, programRunId, user, properties, null);
    return provisioner.getRuntimeJobManager(context);
  }

  /**
   * Record that a cluster will be deprovisioned for a program run, returning a task that will actually perform
   * the cluster deprovisioning. This method must be run within a transaction.
   * The task returned should only be executed after the transaction that ran this method has completed.
   * Running the returned Runnable will start the actual task using an executor within this service so that it can be
   * tracked by this service. The caller does not need to submit the runnable using their own executor.
   *
   * @param programRunId the program run to deprovision
   * @param context context for the transaction
   * @return runnable that will actually execute the cluster deprovisioning
   */
  public Runnable deprovision(ProgramRunId programRunId, StructuredTableContext context) throws IOException {
    return deprovision(programRunId, context, taskStateCleanup);
  }

  // This is visible for testing, where we may not want to delete the task information after it completes
  @VisibleForTesting
  Runnable deprovision(ProgramRunId programRunId, StructuredTableContext context,
                       Consumer<ProgramRunId> taskCleanup) throws IOException {
    // look up information for the corresponding provision operation
    ProvisioningTaskInfo existing =
      provisionerStore.getTaskInfo(new ProvisioningTaskKey(programRunId, ProvisioningOp.Type.PROVISION));
    if (existing == null) {
      runWithProgramLogging(programRunId, Collections.emptyMap(),
                            () -> LOG.error("No task state found while deprovisioning the cluster. "
                                              + "The cluster will be marked as orphaned."));
      programStateWriter.error(programRunId,
                               new IllegalStateException("No task state found while deprovisioning the cluster."));
      provisionerNotifier.orphaned(programRunId);
      return () -> { };
    }

    // cluster can be null if the provisioner was not able to create a cluster. In that case, there is nothing
    // to deprovision, but task state still needs to be cleaned up.
    if (existing.getCluster() == null) {
      provisionerNotifier.deprovisioned(programRunId);
      return () -> taskCleanup.accept(programRunId);
    }

    Provisioner provisioner = provisionerInfo.get().provisioners.get(existing.getProvisionerName());
    if (provisioner == null) {
      runWithProgramLogging(programRunId, existing.getProgramOptions().getArguments().asMap(),
                            () -> LOG.error("Could not deprovision the cluster because provisioner {} does not exist. "
                                              + "The cluster will be marked as orphaned.",
                                            existing.getProvisionerName()));
      programStateWriter.error(programRunId,
                               new IllegalStateException("Provisioner not found while deprovisioning the cluster."));
      provisionerNotifier.orphaned(programRunId);
      return () -> taskCleanup.accept(existing.getProgramRunId());
    }

    ProvisioningOp provisioningOp = new ProvisioningOp(ProvisioningOp.Type.DEPROVISION,
                                                       ProvisioningOp.Status.REQUESTING_DELETE);
    ProvisioningTaskInfo provisioningTaskInfo = new ProvisioningTaskInfo(existing, provisioningOp,
                                                                         existing.getCluster());
    ProvisionerTable provisionerTable = new ProvisionerTable(context);
    provisionerTable.putTaskInfo(provisioningTaskInfo);

    return createDeprovisionTask(provisioningTaskInfo, provisioner, taskCleanup);
  }

  /**
   * Checks if the given provisioner fulfills all the requirements of a program.
   *
   * @param provisionerFulfilledRequirements {@link Requirements} containing requirements fulfilled by the provisioner
   * @param requirements a {@link Set} of {@link PluginRequirement}
   *
   * @return {@link Set} {@link PluginRequirement} which consists of plugin identifier and the requirements which
   * are not meet
   */
  @VisibleForTesting
  Set<PluginRequirement> getUnfulfilledRequirements(Capabilities provisionerFulfilledRequirements,
                                                    Set<PluginRequirement> requirements) {
    // create a map of plugin name to unfulfilled requirement if there are any
    Set<PluginRequirement> unfulfilledRequirements = new HashSet<>();
    Set<String> capabilities =
      provisionerFulfilledRequirements.getDatasetTypes().stream().map(String::toLowerCase).collect(Collectors.toSet());
    for (PluginRequirement pluginRequirement : requirements) {
      Requirements requisites = pluginRequirement.getRequirements();
      Sets.SetView<String> unfulfilledRequirement = Sets.difference(requisites.getDatasetTypes(), capabilities);
      if (!unfulfilledRequirement.isEmpty()) {
        unfulfilledRequirements.add(new PluginRequirement(pluginRequirement.getName(), pluginRequirement.getType(),
                                                          new Requirements(unfulfilledRequirement.immutableCopy())));
      }
    }
    return unfulfilledRequirements;
  }

  /**
   * Aggregates the given {@link Set} of {@link PluginRequirement} on requirements.
   *
   * @param requirements the {@link PluginRequirement} which needs to be aggregated.
   *
   * @return a {@link Map} of requirement to {@link Set} of pluginType:pluginName
   */
  @VisibleForTesting
  Map<String, Set<String>> groupByRequirement(Set<PluginRequirement> requirements) {
    Map<String, Set<String>> pluginsGroupedByRequirements = new HashMap<>();
    for (PluginRequirement pluginRequirement : requirements) {
      Set<String> reqs = pluginRequirement.getRequirements().getDatasetTypes();
      for (String req : reqs) {
        Set<String> currentRequirementPlugins = pluginsGroupedByRequirements.getOrDefault(req, new HashSet<>());
        currentRequirementPlugins.add(pluginRequirement.getType() + ":" + pluginRequirement.getName());
        pluginsGroupedByRequirements.put(req, currentRequirementPlugins);
      }
    }
    return pluginsGroupedByRequirements;
  }

  /**
   * Reloads provisioners in the extension directory. Any new provisioners will be added and any deleted provisioners
   * will be removed. Loaded provisioners will be initialized.
   */
  private void initializeProvisioners() {
    Map<String, Provisioner> provisioners = provisionerProvider.loadProvisioners();
    Map<String, ProvisionerConfig> provisionerConfigs =
      provisionerConfigProvider.loadProvisionerConfigs(provisioners.keySet());
    LOG.debug("Provisioners = {}", provisioners);
    Map<String, ProvisionerDetail> details = new HashMap<>(provisioners.size());
    for (Map.Entry<String, Provisioner> provisionerEntry : provisioners.entrySet()) {
      String provisionerName = provisionerEntry.getKey();
      Provisioner provisioner = provisionerEntry.getValue();
      ProvisionerSystemContext provisionerSystemContext = new DefaultSystemProvisionerContext(cConf, provisionerName);
      try {
        provisioner.initialize(provisionerSystemContext);
      } catch (RuntimeException e) {
        LOG.warn("Error initializing the {} provisioner. It will not be available for use.", provisionerName, e);
        provisioners.remove(provisionerName);
        continue;
      }
      ProvisionerSpecification spec = provisioner.getSpec();
      ProvisionerConfig config = provisionerConfigs.getOrDefault(provisionerName,
                                                                 new ProvisionerConfig(new ArrayList<>(), null, false));
      details.put(provisionerName, new ProvisionerDetail(spec.getName(), spec.getLabel(), spec.getDescription(),
                                                         config.getConfigurationGroups(), config.getIcon(),
                                                         config.isBeta()));
    }
    provisionerInfo.set(new ProvisionerInfo(provisioners, details));
  }

  /**
   * @return unmodifiable collection of all provisioner specs
   */
  public Collection<ProvisionerDetail> getProvisionerDetails() {
    return provisionerInfo.get().details.values();
  }

  /**
   * Get the spec for the specified provisioner.
   *
   * @param name the name of the provisioner
   * @return the spec for the provisioner, or null if the provisioner does not exist
   */
  @Nullable
  public ProvisionerDetail getProvisionerDetail(String name) {
    return provisionerInfo.get().details.get(name);
  }

  /**
   * Validate properties for the specified provisioner.
   *
   * @param provisionerName the name of the provisioner to validate
   * @param properties properties for the specified provisioner
   * @throws NotFoundException if the provisioner does not exist
   * @throws IllegalArgumentException if the properties are invalid
   */
  public void validateProperties(String provisionerName, Map<String, String> properties) throws NotFoundException {
    Provisioner provisioner = provisionerInfo.get().provisioners.get(provisionerName);
    if (provisioner == null) {
      throw new NotFoundException(String.format("Provisioner '%s' does not exist", provisionerName));
    }
    provisioner.validateProperties(properties);
  }

  private Runnable createProvisionTask(ProvisioningTaskInfo taskInfo, Provisioner provisioner) {
    ProgramRunId programRunId = taskInfo.getProgramRunId();
    ProgramOptions programOptions = taskInfo.getProgramOptions();
    Map<String, String> systemArgs = programOptions.getArguments().asMap();
    ProvisionerContext context;
    try {
      SSHContext sshContext = new DefaultSSHContext(Networks.getAddress(cConf, Constants.NETWORK_PROXY_ADDRESS),
                                                    locationFactory.create(taskInfo.getSecureKeysDir()),
                                                    createSSHKeyPair(taskInfo));
      context = createContext(cConf, programOptions, programRunId, taskInfo.getUser(),
                              taskInfo.getProvisionerProperties(), sshContext);
    } catch (IOException e) {
      runWithProgramLogging(taskInfo.getProgramRunId(), systemArgs,
                            () -> LOG.error("Failed to load ssh key. The run will be marked as failed.", e));
      programStateWriter.error(programRunId,
                               new IllegalStateException("Failed to load ssh key.", e));
      provisionerNotifier.deprovisioning(taskInfo.getProgramRunId());
      return () -> { };
    } catch (InvalidMacroException e) {
      runWithProgramLogging(taskInfo.getProgramRunId(), systemArgs,
                            () -> LOG.error("Could not evaluate macros while provisoning. "
                                              + "The run will be marked as failed.", e));
      programStateWriter.error(programRunId,
                               new IllegalStateException("Could not evaluate macros while provisioning", e));
      provisionerNotifier.deprovisioning(taskInfo.getProgramRunId());
      return () -> { };
    }

    // TODO: (CDAP-13246) pick up timeout from profile instead of hardcoding
    ProvisioningTask task = new ProvisionTask(taskInfo, transactionRunner, provisioner, context,
                                              provisionerNotifier, programStateWriter, 300);

    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(programRunId, ProvisioningOp.Type.PROVISION);
    return () -> taskExecutor.submit(taskKey, () -> callWithProgramLogging(programRunId, systemArgs, () -> {
      try {
        return task.executeOnce();
      } catch (InterruptedException e) {
        LOG.debug("Provision task for program run {} interrupted.", taskInfo.getProgramRunId());
        throw e;
      } catch (Exception e) {
        LOG.info("Provision task for program run {} failed.", taskInfo.getProgramRunId(), e);
        throw e;
      }
    }));
  }

  private Runnable createDeprovisionTask(ProvisioningTaskInfo taskInfo, Provisioner provisioner,
                                         Consumer<ProgramRunId> taskCleanup) {
    Map<String, String> properties = taskInfo.getProvisionerProperties();
    ProvisionerContext context;

    SSHKeyPair sshKeyPair = null;
    try {
      sshKeyPair = createSSHKeyPair(taskInfo);
    } catch (IOException e) {
      LOG.warn("Failed to load ssh key. No SSH key will be available for the deprovision task", e);
    }

    ProgramRunId programRunId = taskInfo.getProgramRunId();
    Map<String, String> systemArgs = taskInfo.getProgramOptions().getArguments().asMap();

    try {
      SSHContext sshContext = new DefaultSSHContext(Networks.getAddress(cConf, Constants.NETWORK_PROXY_ADDRESS),
                                                    null, sshKeyPair);
      context = createContext(cConf, taskInfo.getProgramOptions(), programRunId, taskInfo.getUser(), properties,
                              sshContext);
    } catch (InvalidMacroException e) {
      runWithProgramLogging(programRunId, systemArgs,
                            () -> LOG.error("Could not evaluate macros while deprovisoning. "
                                              + "The cluster will be marked as orphaned.", e));
      provisionerNotifier.orphaned(programRunId);
      return () -> { };
    }
    DeprovisionTask task = new DeprovisionTask(taskInfo, transactionRunner, 300,
                                               provisioner, context, provisionerNotifier, locationFactory);
    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(programRunId, ProvisioningOp.Type.DEPROVISION);

    return () -> taskExecutor.submit(taskKey, () -> callWithProgramLogging(programRunId, systemArgs, () -> {
      try {
        long delay = task.executeOnce();
        if (delay < 0) {
          taskCleanup.accept(programRunId);
        }
        return delay;
      } catch (InterruptedException e) {
        // We can get interrupted if the task is cancelled or CDAP is stopped. In either case, just return.
        // If it was cancelled, state cleanup is left to the caller. If it was CDAP master stopping, the task
        // will be resumed on master startup
        LOG.debug("Deprovision task for program run {} interrupted.", programRunId);
        throw e;
      } catch (Exception e) {
        // Otherwise, if there was an error deprovisioning, run the cleanup
        LOG.info("Deprovision task for program run {} failed.", programRunId, e);
        taskCleanup.accept(programRunId);
        throw e;
      }
    }));
  }

  private List<ProvisioningTaskInfo> getInProgressTasks() throws IOException {
    return Retries.callWithRetries(provisionerStore::listTaskInfo, RetryStrategies.fixDelay(6, TimeUnit.SECONDS), t -> {
      // don't retry if we were interrupted, or if the service is not running
      // normally this is only called when the service is starting, but it can be running in unit test
      State serviceState = state();
      if (serviceState != State.STARTING && serviceState != State.RUNNING) {
        return false;
      }
      if (t instanceof InterruptedException) {
        return false;
      }
      // Otherwise always retry, but log unexpected types of failures
      // We expect things like SocketTimeoutException or ConnectException
      // when talking to Dataset Service during startup
      Throwable rootCause = Throwables.getRootCause(t);
      if (!(rootCause instanceof SocketTimeoutException || rootCause instanceof ConnectException)) {
        SAMPLING_LOG.warn("Error scanning for in-progress provisioner tasks. " +
                            "Tasks that were in progress during the last CDAP shutdown will " +
                            "not be resumed until this succeeds. ", t);
      }
      return true;
    });
  }

  /**
   * Creates a {@link SSHKeyPair} based on the given {@link ProvisioningTaskInfo}.
   *
   * @param taskInfo the task info containing information about the ssh keys
   * @return a {@link SSHKeyPair} or {@code null} if ssh key information are not present in the task info
   */
  @Nullable
  private SSHKeyPair createSSHKeyPair(ProvisioningTaskInfo taskInfo) throws IOException {
    // Check if there is ssh user property in the Cluster
    String sshUser = Optional.ofNullable(taskInfo.getCluster())
      .map(Cluster::getProperties)
      .map(p -> p.get(Constants.RuntimeMonitor.SSH_USER))
      .orElse(null);

    if (sshUser == null) {
      return null;
    }

    Location keysDir = locationFactory.create(taskInfo.getSecureKeysDir());

    Location publicKeyLocation = keysDir.append(Constants.RuntimeMonitor.PUBLIC_KEY);
    Location privateKeyLocation = keysDir.append(Constants.RuntimeMonitor.PRIVATE_KEY);

    if (!publicKeyLocation.exists() || !privateKeyLocation.exists()) {
      return null;
    }

    return new LocationBasedSSHKeyPair(keysDir, sshUser);
  }

  /**
   * Creates a {@link Location} representing a directory for storing secure keys for the given program run.
   *
   * @param programRunId the program run for generating the keys directory
   * @return a {@link Location}
   */
  private Location createKeysDirectory(ProgramRunId programRunId) {
    Location keysDir = locationFactory.create(String.format("provisioner/keys/%s.%s.%s.%s.%s",
                                                            programRunId.getNamespace(),
                                                            programRunId.getApplication(),
                                                            programRunId.getType().name().toLowerCase(),
                                                            programRunId.getProgram(),
                                                            programRunId.getRun()));
    try {
      keysDir.mkdirs();
      return keysDir;
    } catch (IOException e) {
      throw new RuntimeException("Failed to create directory " + keysDir, e);
    }
  }

  private ProvisionerContext createContext(CConfiguration cConf, ProgramOptions programOptions,
                                           ProgramRunId programRunId, String userId,
                                           Map<String, String> properties,
                                           @Nullable SSHContext sshContext) {
    RuntimeMonitorType runtimeMonitorType = SystemArguments.getRuntimeMonitorType(cConf, programOptions);
    Map<String, String> systemArgs = programOptions.getArguments().asMap();
    String provisionerName = SystemArguments.getProfileProvisioner(systemArgs);
    String profileName = SystemArguments
      .getProfileIdFromArgs(programRunId.getNamespaceId(), systemArgs)
      .map(ProfileId::getEntityName).orElse(null);
    VersionInfo appCDAPVersion = getAppCDAPVersion(programOptions);
    Map<String, String> evaluated = evaluateMacros(secureStore, userId, programRunId.getNamespace(), properties);
    return new DefaultProvisionerContext(programRunId, provisionerName, evaluated, sparkCompat, sshContext,
                                         appCDAPVersion, locationFactory, runtimeMonitorType,
                                         metricsCollectionService, profileName, contextExecutor);
  }

  /**
   * Evaluates any macros in the property values and returns a copy of the map with the evaluations done.
   * Only the secure macro will be evaluated. Lookups and any other functions will be ignored and left as-is.
   * Secure store reads will be performed as the specified user, which should be the user that actually
   * run program code. This is required because this service runs in the CDAP master, which may be a different user
   * than the program user.
   *
   * @param secureStore the secure store to
   * @param user the user to perform secure store reads as
   * @param namespace the namespace of the program run
   * @param properties the properties to evaluate
   * @return a copy of the input properties with secure macros evaluated
   */
  @VisibleForTesting
  static Map<String, String> evaluateMacros(SecureStore secureStore, String user, String namespace,
                                            Map<String, String> properties) {
    // evaluate macros in the provisioner properties
    MacroEvaluator evaluator = new ProvisionerMacroEvaluator(namespace, secureStore);
    MacroParser macroParser = new MacroParser(evaluator,
                                              MacroParserOptions.builder()
                                                .disableLookups()
                                                .setFunctionWhitelist(ProvisionerMacroEvaluator.SECURE_FUNCTION)
                                                .setEscaping(false)
                                                .build());
    Map<String, String> evaluated = new HashMap<>();

    // do secure store lookups as the user that will run the program
    String oldUser = SecurityRequestContext.getUserId();
    try {
      SecurityRequestContext.setUserId(user);
      for (Map.Entry<String, String> property : properties.entrySet()) {
        String key = property.getKey();
        String val = property.getValue();
        evaluated.put(key, macroParser.parse(val));
      }
    } finally {
      SecurityRequestContext.setUserId(oldUser);
    }
    return evaluated;
  }

  /**
   * Runs the runnable with the logging context set to the program run's context. Anything logged within the runnable
   * will be treated as if they were logs in the program run.
   *
   * @param programRunId the program run to log as
   * @param systemArgs system arguments for the run
   * @param runnable the runnable to run
   */
  private void runWithProgramLogging(ProgramRunId programRunId, Map<String, String> systemArgs, Runnable runnable) {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(programRunId, systemArgs);
    Cancellable cancellable = LoggingContextAccessor.setLoggingContext(loggingContext);
    try {
      runnable.run();
    } finally {
      cancellable.cancel();
    }
  }

  /**
   * Calls the {@link Callable} with the logging context set to the program run's context.
   * Anything logged within the runnable will be treated as if they were logs in the program run.
   *
   * @param programRunId the program run to log as
   * @param systemArgs system arguments for the run
   * @param callable the {@link Callable} to call
   * @return the value returned by the {@link Callable#call()} method
   * @throws Exception if the {@link Callable#call()} method thrown an exception
   */
  private <V> V callWithProgramLogging(ProgramRunId programRunId,
                                       Map<String, String> systemArgs, Callable<V> callable) throws Exception {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(programRunId, systemArgs);
    Cancellable cancellable = LoggingContextAccessor.setLoggingContext(loggingContext);
    try {
      return callable.call();
    } finally {
      cancellable.cancel();
    }
  }

  /**
   * Cancel the task corresponding the the specified key. Returns true if the task was cancelled without completing.
   * Returns false if there was no task for the program run, or it completed before it could be cancelled.
   *
   * @param taskKey the key for the task to cancel
   * @return the state of the task when it was cancelled if the task was running
   */
  private Optional<ProvisioningTaskInfo> cancelTask(ProvisioningTaskKey taskKey,
                                                    Consumer<ProgramRunId> onCancelled) throws IOException {
    Optional<Future<Void>> taskFuture = taskExecutor.getFuture(taskKey);
    if (!taskFuture.isPresent()) {
      return Optional.empty();
    }

    Future<Void> future = taskFuture.get();
    if (future.isDone()) {
      return Optional.empty();
    }

    LOG.trace("Cancelling {} task for program run {}.", taskKey.getType(), taskKey.getProgramRunId());
    if (future.cancel(true)) {
      LOG.debug("Cancelled {} task for program run {}.", taskKey.getType(), taskKey.getProgramRunId());

      // this is the task state after it has been cancelled
      ProvisioningTaskInfo currentTaskInfo = provisionerStore.getExistingAndCancel(taskKey);
      // if the task state was gone by the time we cancelled, it's like we did not cancel it, since it's already
      // effectively done.
      if (currentTaskInfo == null) {
        return Optional.empty();
      } else {
        // run any action that should happen if it was cancelled, like writing a message to TMS
        LOG.trace("Recorded cancelled state for {} task for program run {}.",
                  taskKey.getType(), taskKey.getProgramRunId());
        onCancelled.accept(currentTaskInfo.getProgramRunId());
        return Optional.of(currentTaskInfo);
      }
    }
    return Optional.empty();
  }

  /**
   * Just a container for provisioner instances and specs, so that they can be updated atomically.
   */
  private static class ProvisionerInfo {
    private final Map<String, Provisioner> provisioners;
    private final Map<String, ProvisionerDetail> details;

    private ProvisionerInfo(Map<String, Provisioner> provisioners, Map<String, ProvisionerDetail> details) {
      this.provisioners = Collections.unmodifiableMap(provisioners);
      this.details = Collections.unmodifiableMap(details);
    }
  }
}
