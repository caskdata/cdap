/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.http.NettyHttpService;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Launches an HTTP server for receiving and unpacking and caching artifacts.
 */
public class ArtifactLocalizerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizerService.class);

  private final NettyHttpService httpService;
  private InetSocketAddress bindAddress;
  private ScheduledExecutorService scheduledExecutorService;
  private final ArtifactLocalizerCleaner cleaner;
  private final int cacheCleanupInterval;

  @Inject
  ArtifactLocalizerService(CConfiguration cConf,
                           ArtifactLocalizer artifactLocalizer) {
    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.TASK_WORKER)
      .setHost(InetAddress.getLoopbackAddress().getHostName())
      .setPort(cConf.getInt(Constants.ArtifactLocalizer.PORT))
      .setBossThreadPoolSize(cConf.getInt(Constants.ArtifactLocalizer.BOSS_THREADS))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.ArtifactLocalizer.WORKER_THREADS))
      .setHttpHandlers(new ArtifactLocalizerHttpHandlerInternal(artifactLocalizer));

    httpService = builder.build();
    cacheCleanupInterval = cConf.getInt(Constants.ArtifactLocalizer.CACHE_CLEANUP_INTERVAL_MIN);
    scheduledExecutorService = Executors
      .newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("artifact-cache-cleaner"));
    cleaner = new ArtifactLocalizerCleaner(cConf);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting ArtifactLocalizerService");
    httpService.start();
    scheduledExecutorService.scheduleAtFixedRate(cleaner, cacheCleanupInterval, cacheCleanupInterval, TimeUnit.MINUTES);
    bindAddress = httpService.getBindAddress();
    LOG.debug("Starting ArtifactLocalizerService has completed");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down ArtifactLocalizerService");
    httpService.stop(5, 5, TimeUnit.SECONDS);
    scheduledExecutorService.shutdownNow();
    LOG.debug("Shutting down ArtifactLocalizerService has completed");
  }

  @VisibleForTesting
  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  @VisibleForTesting
  void runCleaner() throws InterruptedException {
    scheduledExecutorService.execute(cleaner);
    scheduledExecutorService.shutdown();
    scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS);
  }
}
