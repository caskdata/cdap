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

package co.cask.cdap.logging.logbuffer;

import co.cask.cdap.api.logging.AppenderContext;
import co.cask.cdap.common.HttpExceptionHandler;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.service.RetryOnStartFailureService;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.logging.framework.LogPipelineLoader;
import co.cask.cdap.logging.framework.LogPipelineSpecification;
import co.cask.cdap.logging.logbuffer.handler.LogBufferHandler;
import co.cask.cdap.logging.meta.CheckpointManager;
import co.cask.cdap.logging.meta.CheckpointManagerFactory;
import co.cask.cdap.logging.pipeline.LogProcessorPipelineContext;
import co.cask.cdap.logging.pipeline.logbuffer.LogBufferPipelineConfig;
import co.cask.cdap.logging.pipeline.logbuffer.LogBufferProcessorPipeline;
import co.cask.http.NettyHttpService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Log Buffer Service responsible for:
 * loading, starting and stopping log buffer pipelines
 * creating concurrent writer
 * starting log buffer cleaner service to clean up logs that have been persisted by log buffer pipelines
 * recovering logs from log buffer
 * starting netty-http service to expose endpoint to process logs.
 */
public class LogBufferService extends AbstractIdleService {
  private final DiscoveryService discoveryService;
  private final CConfiguration cConf;
  private final Provider<AppenderContext> contextProvider;
  private final CheckpointManagerFactory checkpointManagerFactory;
  private final List<Service> pipelines = new ArrayList<>();
  private final List<CheckpointManager<LogBufferFileOffset>> checkpointManagers = new ArrayList<>();

  private Cancellable cancellable;
  private NettyHttpService httpService;
  private ConcurrentLogBufferWriter concurrentWriter;
  private LogBufferRecoveryService recoveryService;

  @Inject
  public LogBufferService(CConfiguration cConf, DiscoveryService discoveryService,
                          CheckpointManagerFactory checkpointManagerFactory,
                          Provider<AppenderContext> contextProvider) {
    this.cConf = cConf;
    this.contextProvider = contextProvider;
    this.checkpointManagerFactory = checkpointManagerFactory;
    this.discoveryService = discoveryService;
  }

  @Override
  protected void startUp() throws Exception {
    // load log pipelines
    List<LogBufferProcessorPipeline> bufferPipelines = loadLogPipelines();
    // start all the log pipelines
    validateAllFutures(Iterables.transform(pipelines, Service::start));

    // start log recovery service to recover all the pending logs
    recoveryService = new LogBufferRecoveryService(cConf, bufferPipelines, checkpointManagers);
    recoveryService.startAndWait();

    // create concurrent writer
    concurrentWriter = new ConcurrentLogBufferWriter(cConf, bufferPipelines);

    // create and start http service
    httpService = createHttpService();
    httpService.start();
    cancellable = discoveryService.register(
      ResolvingDiscoverable.of(new Discoverable(Constants.Service.LOG_BUFFER_SERVICE, httpService.getBindAddress())));
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      if (cancellable != null) {
        cancellable.cancel();
      }
    } finally {
      stopAllServices();
    }
  }

  /**
   * Blocks and validates all the given futures completed successfully.
   */
  private void validateAllFutures(Iterable<? extends ListenableFuture<?>> futures) throws Exception {
    // The get call shouldn't throw exception. It just block until all futures completed.
    Futures.successfulAsList(futures).get();

    // Iterates all futures to make sure all of them completed successfully
    Throwable exception = null;
    for (ListenableFuture<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        if (exception == null) {
          exception = e.getCause();
        } else {
          exception.addSuppressed(e.getCause());
        }
      }
    }
  }

  /**
   * Load log buffer pipelines.
   */
  @SuppressWarnings("unchecked")
  private List<LogBufferProcessorPipeline> loadLogPipelines() {
    Map<String, LogPipelineSpecification<AppenderContext>> specs = new LogPipelineLoader(cConf).load(contextProvider);
    int pipelineCount = specs.size();
    List<LogBufferProcessorPipeline> bufferPipelines = new ArrayList<>();
    // Create one LogBufferProcessorPipeline per spec
    for (LogPipelineSpecification<AppenderContext> pipelineSpec : specs.values()) {
      CConfiguration cConf = pipelineSpec.getConf();
      AppenderContext context = pipelineSpec.getContext();
      long bufferSize = getBufferSize(pipelineCount, cConf);
      LogBufferPipelineConfig config =
        new LogBufferPipelineConfig(bufferSize, cConf.getLong(Constants.Logging.PIPELINE_EVENT_DELAY_MS),
                                    cConf.getLong(Constants.Logging.PIPELINE_CHECKPOINT_INTERVAL_MS),
                                    cConf.getLong(Constants.LogBuffer.LOG_BUFFER_PIPELINE_BATCH_SIZE, 1000));

      CheckpointManager checkpointManager = checkpointManagerFactory.create(pipelineSpec.getCheckpointPrefix(),
                                                                            CheckpointManagerFactory.Type.LOG_BUFFER);
      LogBufferProcessorPipeline pipeline = new LogBufferProcessorPipeline(
        new LogProcessorPipelineContext(cConf, context.getName(), context,
                                        context.getMetricsContext(), context.getInstanceId()), config,
        checkpointManager, 0);
      RetryStrategy retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.log.process.");
      pipelines.add(new RetryOnStartFailureService(() -> pipeline, retryStrategy));
      bufferPipelines.add(pipeline);
      checkpointManagers.add(checkpointManager);
    }

    return bufferPipelines;
  }

  /**
   * Determines the buffer size for one pipeline.
   */
  private long getBufferSize(int numberOfPipelines, CConfiguration cConf) {
    long bufferSize = cConf.getLong(Constants.Logging.PIPELINE_BUFFER_SIZE);
    if (bufferSize > 0) {
      return bufferSize;
    }

    double bufferRatio = cConf.getDouble(Constants.Logging.PIPELINE_AUTO_BUFFER_RATIO);
    Preconditions.checkArgument(bufferRatio > 0 && bufferRatio < 1,
                                "Config %s must be between 0 and 1", Constants.Logging.PIPELINE_AUTO_BUFFER_RATIO);

    bufferSize = (long) ((Runtime.getRuntime().maxMemory() * bufferRatio) / numberOfPipelines);
    return bufferSize > 0 ? bufferSize : 1L;
  }

  private NettyHttpService createHttpService() {
    return new CommonNettyHttpServiceBuilder(cConf, Constants.Service.LOG_BUFFER_SERVICE)
      .setHttpHandlers(new LogBufferHandler(concurrentWriter))
      .setExceptionHandler(new HttpExceptionHandler())
      .setHost(cConf.get(Constants.LogBuffer.LOG_BUFFER_SERVER_BIND_ADDRESS))
      .setPort(cConf.getInt(Constants.LogBuffer.LOG_BUFFER_SERVER_BIND_PORT))
      .build();
  }

  private void stopAllServices() throws Exception {
    if (httpService != null) {
      httpService.stop();
    }
    if (recoveryService != null) {
      recoveryService.stopAndWait();
    }
    // Stops all pipeline
    validateAllFutures(Iterables.transform(pipelines, Service::stop));
  }
}
