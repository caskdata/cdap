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

package io.cdap.cdap.k8s.runtime;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.k8s.common.AbstractWatcherThread;
import io.cdap.cdap.k8s.common.ResourceChangeListener;
import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.util.Config;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;

/**
 * Kubernetes version of a TwillRunner.
 *
 * All resources created by the runner will have a name of the form:
 *
 * [cleansed app name]-[hash]
 *
 * Each resource will also have the following labels:
 *
 * cdap.twill.runner=k8s
 * cdap.twill.run.id=[run id]
 * cdap.twill.app=[cleansed app name]
 *
 * and annotations:
 *
 * cdap.twill.app=[literal app name]
 */
public class KubeTwillRunnerService implements TwillRunnerService {

  static final String START_TIMEOUT_ANNOTATION = "cdap.app.start.timeout.millis";

  private static final Logger LOG = LoggerFactory.getLogger(KubeTwillRunnerService.class);

  static final String APP_LABEL = "cdap.twill.app";
  private static final String RUN_ID_LABEL = "cdap.twill.run.id";
  private static final String RUNNER_LABEL = "cdap.twill.runner";
  private static final String RUNNER_LABEL_VAL = "k8s";

  private final MasterEnvironmentContext masterEnvContext;
  private final String kubeNamespace;
  private final String resourcePrefix;
  private final PodInfo podInfo;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final Map<String, String> extraLabels;
  private final Map<Type, AppResourceWatcherThread<?>> resourceWatchers;
  private final Map<String, KubeLiveInfo> liveInfos;
  private final Lock liveInfoLock;
  private final int jobCleanupIntervalMins;
  private final int jobCleanBatchSize;
  private final String selector;
  private ApiClient apiClient;
  private BatchV1Api batchV1Api;
  private ScheduledExecutorService monitorScheduler;
  private ScheduledExecutorService jobCleanerService;

  public KubeTwillRunnerService(MasterEnvironmentContext masterEnvContext,
                                String kubeNamespace, DiscoveryServiceClient discoveryServiceClient,
                                PodInfo podInfo, String resourcePrefix, Map<String, String> extraLabels,
                                int jobCleanupIntervalMins, int jobCleanBatchSize) {
    this.masterEnvContext = masterEnvContext;
    this.kubeNamespace = kubeNamespace;
    this.podInfo = podInfo;
    this.resourcePrefix = resourcePrefix;
    this.discoveryServiceClient = discoveryServiceClient;
    this.extraLabels = Collections.unmodifiableMap(new HashMap<>(extraLabels));

    // Selects all runs started by the k8s twill runner that has the run id label
    this.selector = String.format("%s=%s,%s", RUNNER_LABEL, RUNNER_LABEL_VAL, RUN_ID_LABEL);
    this.resourceWatchers = ImmutableMap.of(
      V1Deployment.class, AppResourceWatcherThread.createDeploymentWatcher(kubeNamespace, selector),
      V1StatefulSet.class, AppResourceWatcherThread.createStatefulSetWatcher(kubeNamespace, selector),
      V1Job.class, AppResourceWatcherThread.createJobWatcher(kubeNamespace, selector)
    );
    this.liveInfos = new ConcurrentSkipListMap<>();
    this.liveInfoLock = new ReentrantLock();
    this.jobCleanupIntervalMins = jobCleanupIntervalMins;
    this.jobCleanBatchSize = jobCleanBatchSize;
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable) {
    return prepare(runnable, ResourceSpecification.BASIC);
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification) {
    return prepare(new SingleRunnableApplication(runnable, resourceSpecification));
  }

  @Override
  public TwillPreparer prepare(TwillApplication application) {
    TwillSpecification spec = application.configure();
    RunId runId = RunIds.generate();

    Location appLocation = getApplicationLocation(spec.getName(), runId);
    Map<String, String> labels = new HashMap<>(extraLabels);
    labels.put(RUNNER_LABEL, RUNNER_LABEL_VAL);
    labels.put(APP_LABEL, spec.getName());
    labels.put(RUN_ID_LABEL, runId.getId());

    return new KubeTwillPreparer(masterEnvContext, apiClient, kubeNamespace, podInfo,
                                 spec, runId, appLocation, resourcePrefix, labels,
                                 (resourceType, meta, timeout, timeoutUnit) -> {
      // Adds the controller to the LiveInfo.
      liveInfoLock.lock();
      try {
        KubeLiveInfo liveInfo = liveInfos.computeIfAbsent(spec.getName(), n -> new KubeLiveInfo(resourceType, n));
        KubeTwillController controller = createKubeTwillController(spec.getName(), runId, resourceType, meta);
        return liveInfo.addControllerIfAbsent(runId, timeout, timeoutUnit, controller);
      } finally {
        liveInfoLock.unlock();
      }
    });
  }

  @Nullable
  @Override
  public TwillController lookup(String applicationName, RunId runId) {
    KubeLiveInfo liveInfo = liveInfos.get(applicationName);
    return liveInfo == null ? null : liveInfo.getController(runId);
  }

  @Override
  public Iterable<TwillController> lookup(String applicationName) {
    KubeLiveInfo liveInfo = liveInfos.get(applicationName);
    return liveInfo == null ? Collections.emptyList() : liveInfo.getControllers();
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    // Protect against modifications
    return () -> Collections.unmodifiableCollection(liveInfos.values()).stream().map(LiveInfo.class::cast).iterator();
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(@SuppressWarnings("deprecation") SecureStoreUpdater updater,
                                               long initialDelay, long delay, TimeUnit unit) {
    return () -> { };
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer, long initialDelay, long delay, long retryDelay,
                                           TimeUnit unit) {
    return () -> { };
  }

  @Override
  public void start() {
    try {
      apiClient = Config.defaultClient();
      batchV1Api = new BatchV1Api(apiClient);
      monitorScheduler = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("kube-monitor-executor"));
      resourceWatchers.values().forEach(watcher -> {
        watcher.addListener(new AppResourceChangeListener<>());
        watcher.start();
      });

      // start job cleaner service
      jobCleanerService =
        Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("kube-job-cleaner"));
      jobCleanerService.scheduleAtFixedRate(new KubeJobCleaner(batchV1Api, kubeNamespace, selector, jobCleanBatchSize),
                                            10, jobCleanupIntervalMins, TimeUnit.MINUTES);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to get Kubernetes API Client", e);
    }
  }

  @Override
  public void stop() {
    jobCleanerService.shutdownNow();
    resourceWatchers.values().forEach(AbstractWatcherThread::close);
    monitorScheduler.shutdownNow();
  }

  /**
   * Monitors the given {@link KubeTwillController}.
   *
   * @param liveInfo the {@link KubeLiveInfo} that the controller belongs to
   * @param timeout the start timeout
   * @param timeoutUnit the start timeout unit
   * @param controller the controller top monitor
   * @param <T> the type of the resource to watch
   * @param resourceType resource type being controlled by controller
   * @return the controller
   */
  private <T> KubeTwillController monitorController(KubeLiveInfo liveInfo, long timeout,
                                                    TimeUnit timeoutUnit, KubeTwillController controller,
                                                    AppResourceWatcherThread<T> watcher, Type resourceType) {
    String runId = controller.getRunId().getId();

    LOG.debug("Monitoring application {} with run {} starts in {} {}",
              liveInfo.getApplicationName(), runId, timeout, timeoutUnit);

    // Schedule to terminate the controller in the timeout time.
    Future<?> terminationFuture = monitorScheduler.schedule(controller::terminate, timeout, timeoutUnit);

    // This future is for transferring the cancel watch to the change listener
    CompletableFuture<Cancellable> cancellableFuture = new CompletableFuture<>();

    // Listen to resource changes. If the resource represented by the controller has all replicas ready, cancel
    // the terminationFuture. If the resource is deleted, also cancel the terminationFuture, and also terminate
    // the controller as we no longer need to watch for any future changes.
    Cancellable cancellable = watcher.addListener(new ResourceChangeListener<T>() {
      @Override
      public void resourceAdded(T resource) {
        // Handle the same way as modified
        resourceModified(resource);
      }

      @Override
      public void resourceModified(T resource) {
        V1ObjectMeta metadata = getMetadata(resource);
        if (metadata == null) {
          return;
        }

        if (!runId.equals(metadata.getLabels().get(RUN_ID_LABEL))) {
          return;
        }

        if (resourceType.equals(V1Job.class)) {
          // If job has status active we consider it as ready
          if (isJobReady((V1Job) resource)) {
            LOG.debug("Application {} with run {} is available in Kubernetes", liveInfo.getApplicationName(), runId);
            // Cancel the scheduled termination
            terminationFuture.cancel(false);
          }

          // If job is in terminal state - success/failure - we consider it as complete.
          if (isJobComplete((V1Job) resource)) {
            // Cancel the watch
            try {
              Uninterruptibles.getUninterruptibly(cancellableFuture).cancel();
            } catch (ExecutionException e) {
              // This will never happen
            }

            // terminate the job controller
            controller.terminate();
          }
        } else {
          if (isAllReplicasReady(resource)) {
            LOG.debug("Application {} with run {} is available in Kubernetes", liveInfo.getApplicationName(), runId);
            // Cancel the scheduled termination
            terminationFuture.cancel(false);
            // Cancel the watch
            try {
              Uninterruptibles.getUninterruptibly(cancellableFuture).cancel();
            } catch (ExecutionException e) {
              // This will never happen
            }
          }
        }
      }

      @Override
      public void resourceDeleted(T resource) {
        V1ObjectMeta metadata = getMetadata(resource);
        if (metadata == null) {
          return;
        }

        // If the run is deleted, terminate the controller right away and cancel the scheduled termination
        if (runId.equals(metadata.getLabels().get(RUN_ID_LABEL))) {
          // Cancel the scheduled termination
          terminationFuture.cancel(false);
          controller.terminate();
          // Cancel the watch
          try {
            Uninterruptibles.getUninterruptibly(cancellableFuture).cancel();
          } catch (ExecutionException e) {
            // This will never happen
          }
        }
      }
    });

    cancellableFuture.complete(cancellable);

    // On controller termination, remove it from the liveInfo
    controller.onTerminated(() -> {
      // Cancel the scheduled termination
      terminationFuture.cancel(false);
      // Cancel the watch if there is one
      cancellable.cancel();
      liveInfoLock.lock();
      try {
        liveInfo.removeController(controller);
        if (liveInfo.isEmpty()) {
          liveInfos.remove(liveInfo.getApplicationName(), liveInfo);
        }
      } finally {
        liveInfoLock.unlock();
      }

      if (!resourceType.equals(V1Job.class)) {
        try {
          Uninterruptibles.getUninterruptibly(controller.terminate());
          LOG.debug("Controller for application {} of run {} is terminated", liveInfo.getApplicationName(), runId);
        } catch (ExecutionException e) {
          LOG.error("Controller for application {} of run {} is terminated due to failure",
                    liveInfo.getApplicationName(), runId, e.getCause());
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    return controller;
  }

  /**
   * Returns the application {@link Location} for staging files.
   *
   * @param name name of the twill application
   * @param runId the runId
   * @return the application {@link Location}
   */
  private Location getApplicationLocation(String name, RunId runId) {
    return masterEnvContext.getLocationFactory().create(String.format("twill/%s/%s", name, runId.getId()));
  }

  /**
   * Extracts a {@link V1ObjectMeta} from the given k8s resource or returns {@code null} if the given resource object
   * doesn't contain metadata.
   */
  @Nullable
  private V1ObjectMeta getMetadata(Object resource) {
    if (resource instanceof KubernetesObject) {
      return ((KubernetesObject) resource).getMetadata();
    }
    LOG.warn("Failed to extract object metadata from resource {}", resource);
    return null;
  }

  /**
   * Checks if number of requested replicas is the same as the number of ready replicas in the given resource.
   */
  private boolean isAllReplicasReady(Object resource) {
    try {
      Method getStatus = resource.getClass().getDeclaredMethod("getStatus");
      Object statusObj = getStatus.invoke(resource);

      Method getReplicas = statusObj.getClass().getDeclaredMethod("getReplicas");
      Integer replicas = (Integer) getReplicas.invoke(statusObj);

      Method getReadyReplicas = statusObj.getClass().getDeclaredMethod("getReadyReplicas");
      Integer readyReplicas = (Integer) getReadyReplicas.invoke(statusObj);

      return replicas != null && Objects.equals(replicas, readyReplicas);

    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | ClassCastException e) {
      LOG.warn("Failed to get number of replicas and ready replicas from the resource {}", resource, e);
      return false;
    }
  }

  /**
   * Checks if job is ready.
   */
  private boolean isJobReady(V1Job job) {
    V1JobStatus jobStatus = job.getStatus();
    if (jobStatus != null) {
      Integer active = jobStatus.getActive();
      return active != null;
    }
    return false;
  }

  /**
   * Checks if job is complete. Job completion can be in success or failed state.
   */
  private boolean isJobComplete(V1Job job) {
    V1JobStatus jobStatus = job.getStatus();
    if (jobStatus != null) {
      return jobStatus.getFailed() != null || jobStatus.getSucceeded() != null;
    }
    return false;
  }

  /**
   * An {@link ResourceChangeListener} to watch for changes in application resources. It is for refreshing the
   * liveInfos map.
   */
  private final class AppResourceChangeListener<T> implements ResourceChangeListener<T> {

    @Override
    public void resourceAdded(T resource) {
      V1ObjectMeta metadata = getMetadata(resource);
      if (metadata == null) {
        return;
      }
      String appName = metadata.getAnnotations().get(APP_LABEL);
      if (appName == null) {
        // This shouldn't happen. Just to guard against future bug.
        return;
      }
      String resourceName = metadata.getName();
      RunId runId = RunIds.fromString(metadata.getLabels().get(RUN_ID_LABEL));

      // Read the start timeout millis from the annotation. It is set by the KubeTwillPreparer
      long startTimeoutMillis = TimeUnit.SECONDS.toMillis(120);
      try {
        startTimeoutMillis = Long.parseLong(metadata.getAnnotations().get(START_TIMEOUT_ANNOTATION));
      } catch (Exception e) {
        // This shouldn't happen
        LOG.warn("Failed to get start timeout from the annotation using key {} from resource {}. Defaulting to {} ms",
                 START_TIMEOUT_ANNOTATION, metadata.getName(), startTimeoutMillis, e);
      }

      // Add the LiveInfo and Controller
      liveInfoLock.lock();
      try {
        KubeLiveInfo liveInfo = liveInfos.computeIfAbsent(appName, k -> new KubeLiveInfo(resource.getClass(), appName));
        KubeTwillController controller = createKubeTwillController(appName, runId, resource.getClass(), metadata);
        liveInfo.addControllerIfAbsent(runId, startTimeoutMillis, TimeUnit.MILLISECONDS, controller);
      } finally {
        liveInfoLock.unlock();
      }
    }

    @Override
    public void resourceDeleted(T resource) {
      V1ObjectMeta metadata = getMetadata(resource);
      if (metadata == null) {
        return;
      }
      String appName = metadata.getAnnotations().get(APP_LABEL);
      if (appName == null) {
        // This shouldn't happen. Just to guard against future bug.
        return;
      }

      // Get and terminate the controller
      liveInfoLock.lock();
      try {
        KubeLiveInfo liveInfo = liveInfos.get(appName);
        if (liveInfo != null) {
          RunId runId = RunIds.fromString(metadata.getLabels().get(RUN_ID_LABEL));
          if (!liveInfo.resourceType.equals(V1Job.class)) {
            Optional.ofNullable(liveInfo.getController(runId)).ifPresent(KubeTwillController::terminate);
          }
        }
      } finally {
        liveInfoLock.unlock();
      }
    }
  }

  /**
   * Creates a {@link KubeTwillController}.
   */
  private KubeTwillController createKubeTwillController(String appName, RunId runId,
                                                        Type resourceType, V1ObjectMeta meta) {
    KubeTwillController controller = new KubeTwillController(kubeNamespace, runId, discoveryServiceClient,
                                                             apiClient, resourceType, meta);

    Location appLocation = getApplicationLocation(appName, runId);
    controller.onTerminated(() -> {
      try {
        appLocation.delete(true);
      } catch (IOException e) {
        throw new RuntimeException("Failed to delete location for " + appName + "-" + runId + " at " + appLocation, e);
      }
    }, command -> new Thread(command, "app-cleanup-" + appName).start());
    return controller;
  }

  /**
   * A TwillApplication with a single runnable.
   */
  private static class SingleRunnableApplication implements TwillApplication {

    private final TwillRunnable runnable;
    private final ResourceSpecification resourceSpec;

    SingleRunnableApplication(TwillRunnable runnable, ResourceSpecification resourceSpec) {
      this.runnable = runnable;
      this.resourceSpec = resourceSpec;
    }

    @Override
    public TwillSpecification configure() {
      TwillRunnableSpecification runnableSpec = runnable.configure();
      return TwillSpecification.Builder.with()
        .setName(runnableSpec.getName())
        .withRunnable().add(runnableSpec.getName(), runnable, resourceSpec)
        .noLocalFiles()
        .anyOrder()
        .build();
    }
  }

  /**
   * Kubernetes LiveInfo.
   */
  private final class KubeLiveInfo implements LiveInfo {
    private final Type resourceType;
    private final String applicationName;
    private final Map<String, KubeTwillController> controllers;

    KubeLiveInfo(Type resourceType, String applicationName) {
      this.resourceType = resourceType;
      this.applicationName = applicationName;
      this.controllers = new ConcurrentSkipListMap<>();
    }

    KubeTwillController addControllerIfAbsent(RunId runId, long timeout, TimeUnit timeoutUnit,
                                              KubeTwillController controller) {
      KubeTwillController existing = controllers.putIfAbsent(runId.getId(), controller);
      if (existing != null) {
        return existing;
      }
      // If it is newly added controller, monitor it.
      return monitorController(this, timeout, timeoutUnit, controller, resourceWatchers.get(resourceType),
                               resourceType);
    }

    /**
     * Remove the given controller instance.
     * @param controller the instance to remove
     */
    void removeController(KubeTwillController controller) {
      controllers.remove(controller.getRunId().getId(), controller);
    }

    boolean isEmpty() {
      return controllers.isEmpty();
    }

    @Override
    public String getApplicationName() {
      return applicationName;
    }

    @Override
    public Iterable<TwillController> getControllers() {
      // Protect against modifications
      return () -> Collections.unmodifiableCollection(controllers.values())
        .stream().map(TwillController.class::cast).iterator();
    }

    /**
     * Returns the {@link KubeTwillController} for the given {@link RunId}.
     */
    @Nullable
    KubeTwillController getController(RunId runId) {
      return controllers.get(runId.getId());
    }
  }

}
