/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.spark.SparkProgram;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.app.metrics.WorkflowMetrics;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.program.ProgramTypeMetricTag;
import co.cask.cdap.internal.app.runtime.DefaultAdmin;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.spark.metrics.SparkUserMetrics;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.cdap.logging.context.SparkLoggingContext;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An abstract implementation of {@link SparkContext} for common functionality that spread across
 * execution context for {@link Spark} and {@link SparkProgram}.
 */
public abstract class AbstractSparkContext implements SparkContext, Closeable {

  private final ApplicationSpecification applicationSpecification;
  private final SparkSpecification specification;
  private final Id.Program programId;
  private final RunId runId;
  private final ClassLoader programClassLoader;
  private final long logicalStartTime;
  private final Map<String, String> runtimeArguments;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final MetricsContext programMetricsContext;
  private final MetricsContext workFlowMetricsContext;
  private final LoggingContext loggingContext;
  private final PluginInstantiator pluginInstantiator;
  private final Admin admin;
  protected final SystemDatasetInstantiator systemDatasetInstantiator;
  private final WorkflowProgramInfo workflowProgramInfo;
  private final MetricsCollectionService metricsCollectionService;

  private Resources executorResources;
  private SparkConf sparkConf;

  protected AbstractSparkContext(ApplicationSpecification applicationSpecification,
                                 SparkSpecification specification, Id.Program programId, RunId runId,
                                 ClassLoader programClassLoader,
                                 Map<String, String> runtimeArguments, DiscoveryServiceClient discoveryServiceClient,
                                 MetricsContext metricsContext, LoggingContext loggingContext,
                                 DatasetFramework dsFramework,
                                 MetricsCollectionService metricsCollectionService,
                                 @Nullable PluginInstantiator pluginInstantiator,
                                 @Nullable WorkflowProgramInfo workflowProgramInfo) {
    this.applicationSpecification = applicationSpecification;
    this.specification = specification;
    this.programId = programId;
    this.runId = runId;
    this.programClassLoader = programClassLoader;
    this.discoveryServiceClient = discoveryServiceClient;
    this.metricsCollectionService = metricsCollectionService;
    this.programMetricsContext = metricsContext;
    this.workFlowMetricsContext = getWorkflowMetricsContext(workflowProgramInfo, programId, metricsCollectionService);
    this.loggingContext = loggingContext;
    this.executorResources = Objects.firstNonNull(specification.getExecutorResources(), new Resources());
    this.sparkConf = new SparkConf();
    this.pluginInstantiator = pluginInstantiator;
    this.workflowProgramInfo = workflowProgramInfo;
    this.systemDatasetInstantiator = new SystemDatasetInstantiator(dsFramework, programClassLoader, getOwners());
    this.admin = new DefaultAdmin(dsFramework, programId.getNamespace().toEntityId());


    Map<String, String> runtimeArgs = new HashMap<>(runtimeArguments);
    this.logicalStartTime = ProgramRunners.updateLogicalStartTime(runtimeArgs);
    this.runtimeArguments = Collections.unmodifiableMap(runtimeArgs);
  }

  @Override
  public ApplicationSpecification getApplicationSpecification() {
    return applicationSpecification;
  }

  @Override
  public String getNamespace() {
    return programId.getNamespaceId();
  }

  @Override
  public SparkSpecification getSpecification() {
    return specification;
  }

  @Override
  public long getLogicalStartTime() {
    return logicalStartTime;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  @Override
  public ServiceDiscoverer getServiceDiscoverer() {
    return new SparkServiceDiscoverer(getProgramId().toEntityId(), discoveryServiceClient);
  }

  @Override
  public Metrics getMetrics() {
    return new SparkUserMetrics(getProgramWorkflowMetrics());
  }

  @Override
  public PluginContext getPluginContext() {
    return new SparkPluginContext(pluginInstantiator, programId.toEntityId(), applicationSpecification.getPlugins());
  }

  @Override
  public <T> T readFromDataset(String datasetName, Class<?> kClass, Class<?> vClass) {
    return readFromDataset(datasetName, kClass, vClass, Collections.<String, String>emptyMap());
  }

  @Override
  public <T> void writeToDataset(T rdd, String datasetName, Class<?> kClass, Class<?> vClass) {
    writeToDataset(rdd, datasetName, kClass, vClass, Collections.<String, String>emptyMap());
  }

  @Override
  public <T> T readFromStream(String streamName, Class<?> vClass) {
    return readFromStream(streamName, vClass, 0, System.currentTimeMillis());
  }

  @Override
  public <T> T readFromStream(String streamName, Class<?> vClass, long startTime, long endTime) {
    return readFromStream(streamName, vClass, startTime, endTime, null);
  }

  @Override
  public <T> T readFromStream(String streamName, Class<?> vClass, long startTime, long endTime,
                              @Nullable Class<? extends StreamEventDecoder> decoderType) {

    StreamBatchReadable stream = (decoderType == null)
      ? new StreamBatchReadable(streamName, startTime, endTime)
      : new StreamBatchReadable(streamName, startTime, endTime, decoderType);

    return readFromStream(stream, vClass);
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return getDataset(name, RuntimeArguments.extractScope(Scope.DATASET, name, getRuntimeArguments()));
  }

  @Override
  public void setExecutorResources(Resources resources) {
    Preconditions.checkArgument(resources != null, "Resources must not be null");
    this.executorResources = resources;
  }

  @Nullable
  @Override
  public WorkflowToken getWorkflowToken() {
    return workflowProgramInfo == null ? null : workflowProgramInfo.getWorkflowToken();
  }

  @Override
  public <T> void setSparkConf(T sparkConf) {
    Preconditions.checkArgument(sparkConf instanceof SparkConf, "Invalid config type %s. Only accept %s.",
                                sparkConf.getClass().getName(), SparkConf.class.getName());
    this.sparkConf = (SparkConf) sparkConf;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(SparkContext.class)
      .add("id", getProgramId())
      .add("runId", getRunId())
      .toString();
  }

  /**
   * Returns the Spark program Id.
   */
  public Id.Program getProgramId() {
    return programId;
  }

  /**
   * Returns the {@link RunId} of the run represented by this context.
   */
  @Override
  public RunId getRunId() {
    return runId;
  }

  /**
   * Returns the {@link ClassLoader} for the Spark program.
   */
  public ClassLoader getProgramClassLoader() {
    return programClassLoader;
  }

  /**
   * Returns the {@link DiscoveryServiceClient} for this context.
   */
  public DiscoveryServiceClient getDiscoveryServiceClient() {
    return discoveryServiceClient;
  }

  /**
   * Returns the underlying {@link Metrics} instance for emitting user metrics. The returned instance is
   * not serializable and shouldn't be exposed to user program directly.
   */
  public Metrics getProgramWorkflowMetrics() {
    if (workFlowMetricsContext != null) {
      return new WorkflowMetrics(programMetricsContext, workFlowMetricsContext);
    } else {
      return new ProgramUserMetrics(programMetricsContext);
    }
  }

  /**
   * Returns the {@link MetricsContext} for this context.
   */
  public MetricsContext getProgramMetricsContext() {
    return programMetricsContext;
  }

  /**
   * Returns the {@link PluginInstantiator} for this context.
   */
  @Nullable
  public PluginInstantiator getPluginInstantiator() {
    return pluginInstantiator;
  }

  /**
   * Returns the {@link WorkflowProgramInfo} for this context or null if not running inside workflow.
   */
  @Nullable
  public WorkflowProgramInfo getWorkflowProgramInfo() {
    return workflowProgramInfo;
  }

  /**
   * Returns the {@link LoggingContext} for this context.
   */
  public LoggingContext getLoggingContext() {
    return loggingContext;
  }

  /**
   * Returns the {@link Resources} requirement for the executor.
   */
  public Resources getExecutorResources() {
    return executorResources;
  }

  /**
   * Returns the {@link SparkConf} for the spark program.
   */
  public SparkConf getSparkConf() {
    return sparkConf;
  }

  /**
   * Returns the {@link List} of {@link Id} objects representing dataset owner of this context.
   */
  protected List<? extends Id> getOwners() {
    return ImmutableList.of(getProgramId());
  }

  /**
   * Helper method for creating {@link MetricsContext} from {@link MetricsCollectionService} based on
   * the given program execution context.
   */
  protected static MetricsContext createMetricsContext(MetricsCollectionService service,
                                                       Id.Program programId, RunId runId) {
    Map<String, String> tags = Maps.newHashMap();
    tags.put(Constants.Metrics.Tag.NAMESPACE, programId.getNamespaceId());
    tags.put(Constants.Metrics.Tag.APP, programId.getApplicationId());
    tags.put(ProgramTypeMetricTag.getTagName(ProgramType.SPARK), programId.getId());
    tags.put(Constants.Metrics.Tag.RUN_ID, runId.getId());

    // todo: use proper spark instance id. For now we have to emit smth for test framework's waitFor metric to work
    tags.put(Constants.Metrics.Tag.INSTANCE_ID, "0");

    return service.getContext(tags);
  }

  /**
   * get the metrics collection service
   * @return metrics collection service
   */
  public MetricsCollectionService getMetricsCollectionService() {
    return metricsCollectionService;
  }

  @Nullable
  private MetricsContext getWorkflowMetricsContext(@Nullable WorkflowProgramInfo workflowProgramInfo,
                                                   Id.Program program,
                                                   MetricsCollectionService metricsCollectionService) {
    if (workflowProgramInfo == null) {
      return null;
    }

    Map<String, String> tags = Maps.newHashMap();
    tags.put(Constants.Metrics.Tag.NAMESPACE, program.getNamespaceId());
    tags.put(Constants.Metrics.Tag.APP, program.getApplicationId());
    tags.put(Constants.Metrics.Tag.WORKFLOW, workflowProgramInfo.getName());
    tags.put(Constants.Metrics.Tag.RUN_ID, workflowProgramInfo.getRunId().getId());
    tags.put(Constants.Metrics.Tag.NODE, workflowProgramInfo.getNodeId());
    return metricsCollectionService.getContext(tags);
  }

  /**
   * Helper method for creating {@link LoggingContext} based on the given program execution context.
   */
  protected static LoggingContext createLoggingContext(Id.Program programId, RunId runId) {
    return new SparkLoggingContext(programId.getNamespaceId(), programId.getApplicationId(),
                                   programId.getId(), runId.getId());
  }

  @Override
  public Admin getAdmin() {
    return admin;
  }

}
