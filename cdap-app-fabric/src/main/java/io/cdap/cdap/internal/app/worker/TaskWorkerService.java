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

package io.cdap.cdap.internal.app.worker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.worker.api.RunnableTask;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.http.NettyHttpService;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Launches an HTTP server for receiving and handling {@link RunnableTask}
 */
public class TaskWorkerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerService.class);

  private final CConfiguration cConf;
  private final SConfiguration sConf;
  private final Configuration hConf;
  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private final ArtifactRepositoryReader artifactRepositoryReader;
  private final ArtifactRepository artifactRepository;
  private final Impersonator impersonator;
  private Cancellable cancelDiscovery;
  private InetSocketAddress bindAddress;


  @Inject
  TaskWorkerService(CConfiguration cConf,
                    Configuration hConf,
                    SConfiguration sConf,
                    DiscoveryService discoveryService,
                    ArtifactRepositoryReader artifactRepositoryReader,
                    ArtifactRepository artifactRepository,
                    Impersonator impersonator) {
    this.cConf = cConf;
    this.sConf = sConf;
    this.hConf = hConf;
    this.discoveryService = discoveryService;
    this.artifactRepositoryReader = artifactRepositoryReader;
    this.artifactRepository = artifactRepository;
    this.impersonator = impersonator;

    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.TASK_WORKER)
      .setHost(cConf.get(Constants.TaskWorker.ADDRESS))
      .setPort(cConf.getInt(Constants.TaskWorker.PORT))
      .setExecThreadPoolSize(cConf.getInt(Constants.TaskWorker.EXEC_THREADS))
      .setBossThreadPoolSize(cConf.getInt(Constants.TaskWorker.BOSS_THREADS))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.TaskWorker.WORKER_THREADS))
      .setHttpHandlers(new TaskWorkerHttpHandlerInternal(this.cConf, this.hConf, artifactRepositoryReader,
                                                         artifactRepository, impersonator, this::stopService));

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }
    httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting TaskWorkerService");
    httpService.start();
    bindAddress = httpService.getBindAddress();
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.TASK_WORKER, httpService)));
    LOG.info("Starting TaskWorkerService has completed");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down TaskWorkerService");
    cancelDiscovery.cancel();
    httpService.stop(5, 5, TimeUnit.SECONDS);
    LOG.debug("Shutting down TaskWorkerService has completed");
  }

  private void stopService(String className) {
    /** TODO: Expand this logic such that
     * based on number of requests per particular class,
     * the service gets stopped.
     */
    stop();
  }

  @VisibleForTesting
  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }
}
