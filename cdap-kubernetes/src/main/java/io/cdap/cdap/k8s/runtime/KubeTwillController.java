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

import io.cdap.cdap.master.spi.twill.ExtendedTwillController;
import io.kubernetes.client.openapi.ApiCallback;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1DeleteOptions;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Preconditions;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1Status;
import org.apache.twill.api.Command;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceController;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

/**
 * Kubernetes version of a TwillController.
 */
class KubeTwillController implements ExtendedTwillController {

  private static final Logger LOG = LoggerFactory.getLogger(KubeTwillController.class);

  private final String kubeNamespace;
  private final RunId runId;
  private final CompletableFuture<KubeTwillController> completion;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final ApiClient apiClient;
  private final BatchV1Api batchV1Api;
  private final Type resourceType;
  private final V1ObjectMeta meta;

  KubeTwillController(String kubeNamespace, RunId runId, DiscoveryServiceClient discoveryServiceClient,
                      ApiClient apiClient, Type resourceType, V1ObjectMeta meta) {
    this.kubeNamespace = kubeNamespace;
    this.runId = runId;
    this.completion = new CompletableFuture<>();
    this.discoveryServiceClient = discoveryServiceClient;
    this.apiClient = apiClient;
    this.batchV1Api = new BatchV1Api(apiClient);
    this.resourceType = resourceType;
    this.meta = meta;
  }

  @Override
  public void addLogHandler(LogHandler handler) {
    // no-op
  }

  @Override
  public ServiceDiscovered discoverService(String serviceName) {
    return discoveryServiceClient.discover(serviceName);
  }

  @Override
  public Future<Integer> changeInstances(String runnable, int newCount) {
    // TODO: implement with AppsV1Api.patchNamespacedDeploymentAsync()
    CompletableFuture<Integer> f = new CompletableFuture<>();
    f.completeExceptionally(new UnsupportedOperationException("Change instances is currently not supported"));
    return f;
  }

  @Nullable
  @Override
  public ResourceReport getResourceReport() {
    return null;
  }

  @Override
  public Future<String> restartAllInstances(String runnable) {
    CompletableFuture<String> resultFuture = new CompletableFuture<>();
    CoreV1Api api = new CoreV1Api(apiClient);

    try {
      api.deleteCollectionNamespacedPodAsync(kubeNamespace, null, null, null, null, null, getLabelSelector(),
                                             null, null, null, null, null, null, null,
                                             createCallbackFutureAdapter(resultFuture, r -> runnable));
    } catch (ApiException e) {
      completeExceptionally(resultFuture, e);
    }

    return resultFuture;
  }

  @Override
  public Future<Set<String>> restartInstances(Map<String, ? extends Set<Integer>> runnableToInstanceIds) {
    CompletableFuture<Set<String>> future = new CompletableFuture<>();

    // Since we only support one TwillRunnable, the runnable name is ignored
    if (runnableToInstanceIds.isEmpty()) {
      future.complete(Collections.emptySet());
      return future;
    }
    if (runnableToInstanceIds.size() != 1) {
      future.completeExceptionally(new UnsupportedOperationException("Only one runnable is supported"));
      return future;
    }
    Map.Entry<String, ? extends Set<Integer>> entry = runnableToInstanceIds.entrySet().iterator().next();
    return doRestartInstances(entry.getKey(), entry.getValue()).thenApply(s -> Collections.singleton(entry.getKey()));
  }

  @Override
  public Future<String> restartInstances(String runnable, int instanceId, int... moreInstanceIds) {
    return restartInstances(runnable,
                            IntStream.concat(IntStream.of(instanceId),
                                             IntStream.of(moreInstanceIds)).boxed().collect(Collectors.toSet()));
  }

  @Override
  public Future<String> restartInstances(String runnable, Set<Integer> instanceIds) {
    return doRestartInstances(runnable, instanceIds);
  }

  @Override
  public Future<String> restartInstance(String runnable, int instanceId, String uid) {
    CompletableFuture<String> resultFuture = new CompletableFuture<>();
    CoreV1Api api = new CoreV1Api(apiClient);

    // Only support for stateful set for now
    if (!V1StatefulSet.class.equals(resourceType)) {
      resultFuture.completeExceptionally(
        new UnsupportedOperationException("Instance restart by instance id is only supported for statefulsets"));
      return resultFuture;
    }

    V1DeleteOptions deleteOptions = new V1DeleteOptions().preconditions(new V1Preconditions().uid(uid));
    String podName = String.format("%s-%d", meta.getName(), instanceId);

    try {
      api.deleteNamespacedPodAsync(podName, kubeNamespace, null, null, null, null, null, deleteOptions,
                                   createCallbackFutureAdapter(resultFuture, r -> runnable));
    } catch (ApiException e) {
      completeExceptionally(resultFuture, e);
    }
    return resultFuture;
  }

  @Override
  public Future<Map<String, LogEntry.Level>> updateLogLevels(Map<String, LogEntry.Level> logLevels) {
    CompletableFuture<Map<String, LogEntry.Level>> f = new CompletableFuture<>();
    f.completeExceptionally(new UnsupportedOperationException("updateLogLevels is currently not supported"));
    return f;
  }

  @Override
  public Future<Map<String, LogEntry.Level>> updateLogLevels(String runnableName,
                                                             Map<String, LogEntry.Level> logLevelsForRunnable) {
    CompletableFuture<Map<String, LogEntry.Level>> f = new CompletableFuture<>();
    f.completeExceptionally(new UnsupportedOperationException("updateLogLevels is currently not supported"));
    return f;
  }

  @Override
  public Future<String[]> resetLogLevels(String... loggerNames) {
    CompletableFuture<String[]> f = new CompletableFuture<>();
    f.completeExceptionally(new UnsupportedOperationException("resetLogLevels is currently not supported"));
    return f;
  }

  @Override
  public Future<String[]> resetRunnableLogLevels(String runnableName, String... loggerNames) {
    CompletableFuture<String[]> f = new CompletableFuture<>();
    f.completeExceptionally(new UnsupportedOperationException("resetLogLevels is currently not supported"));
    return f;
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public Future<Command> sendCommand(Command command) {
    CompletableFuture<Command> f = new CompletableFuture<>();
    f.completeExceptionally(new UnsupportedOperationException("sendCommand is currently not supported"));
    return f;
  }

  @Override
  public Future<Command> sendCommand(String runnableName, Command command) {
    CompletableFuture<Command> f = new CompletableFuture<>();
    f.completeExceptionally(new UnsupportedOperationException("sendCommand is currently not supported"));
    return f;
  }

  @Override
  public Future<? extends ServiceController> terminate() {
    if (completion.isDone()) {
      return completion;
    }

    return resourceType.equals(V1Job.class) ? cleanupJob() : cleanupResources();
  }

  @Override
  public void kill() {
    terminate();
  }

  @Override
  public void onRunning(Runnable runnable, Executor executor) {
    executor.execute(runnable);
  }

  @Override
  public void onTerminated(Runnable runnable, Executor executor) {
    completion.whenCompleteAsync((controller, throwable) -> runnable.run(), executor);
  }

  @Override
  public void awaitTerminated() throws ExecutionException {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          completion.get();
          return;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void awaitTerminated(long timeout, TimeUnit timeoutUnit) throws TimeoutException, ExecutionException {
    boolean interrupted = false;
    try {
      long remainingNanos = timeoutUnit.toNanos(timeout);
      long end = System.nanoTime() + remainingNanos;

      while (true) {
        try {
          completion.get(remainingNanos, TimeUnit.NANOSECONDS);
          return;
        } catch (InterruptedException e) {
          interrupted = true;
          remainingNanos = end - System.nanoTime();
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Nullable
  @Override
  public TerminationStatus getTerminationStatus() {
    if (!completion.isDone()) {
      return null;
    }
    try {
      awaitTerminated();
      return TerminationStatus.SUCCEEDED;
    } catch (ExecutionException e) {
      return TerminationStatus.FAILED;
    }
  }

  /**
   * Creates a label selector that matches all labels in the meta object.
   */
  private String getLabelSelector() {
    return meta.getLabels().entrySet().stream()
      .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
      .collect(Collectors.joining(","));
  }

  /**
   * Restarts the given set of instances. This method currently only supports restart of stateful set.
   *
   * @param runnable name of the runnable to restart. This is currently unused
   * @param instanceIds the set of instance ids to restart
   * @return a {@link CompletableFuture} that will complete when the delete operation completed
   */
  private CompletableFuture<String> doRestartInstances(String runnable, Set<Integer> instanceIds) {
    CompletableFuture<String> resultFuture = new CompletableFuture<>();
    CoreV1Api api = new CoreV1Api(apiClient);

    // Only support for stateful set for now
    if (!V1StatefulSet.class.equals(resourceType)) {
      resultFuture.completeExceptionally(
        new UnsupportedOperationException("Instance restart by instance id is only supported for deployment"));
      return resultFuture;
    }

    // For stateful set, it can be done by deleting pods by the selected names formed by the instance ids.
    String labelSelector = "statefulset.kubernetes.io/pod-name in " +
      instanceIds.stream()
        .map(i -> String.format("%s-%d", meta.getName(), i))
        .collect(Collectors.joining(",", "(", ")"));

    try {
      api.deleteCollectionNamespacedPodAsync(kubeNamespace, null, null, null, null, null,
                                             labelSelector, null, null, null, null, null, null,
                                             null, createCallbackFutureAdapter(resultFuture, r -> runnable));
    } catch (ApiException e) {
      completeExceptionally(resultFuture, e);
    }
    return resultFuture;
  }

  /**
   * Creates a {@link ApiCallback} with the callback result adapted back to the provided {@link CompletableFuture}.
   *
   * @param <T> type of the result
   * @param <R> type of the result for the future
   */
  private <T, R> ApiCallback<T> createCallbackFutureAdapter(CompletableFuture<R> future, Function<T, R> func) {
    return new ApiCallbackAdapter<T>() {
      @Override
      public void onFailure(ApiException e, int statusCode, Map responseHeaders) {
        completeExceptionally(future, e);
      }

      @Override
      public void onSuccess(T result, int statusCode, Map<String, List<String>> responseHeaders) {
        future.complete(func.apply(result));
      }
    };
  }

  /**
   * Deletes the underlying resource in k8s represented by this controller.
   *
   * @return a {@link CompletionStage} that will complete when the delete operation completed
   */
  private CompletionStage<String> deleteResource() {
    if (V1Deployment.class.equals(resourceType)) {
      return deleteDeployment();
    }
    if (V1StatefulSet.class.equals(resourceType)) {
      return deleteStatefulSet();
    }
    // This shouldn't happen
    throw new UnsupportedOperationException("Cannot delete resource of type " + resourceType);
  }

  /**
   * Deletes the deployment controlled by this controller asynchronously.
   *
   * @return a {@link CompletionStage} that will complete when the delete operation completed
   */
  private CompletionStage<String> deleteDeployment() {
    LOG.debug("Deleting Deployment {}", meta.getName());

    AppsV1Api appsApi = new AppsV1Api(apiClient);

    // callback for the delete deployment call
    CompletableFuture<String> resultFuture = new CompletableFuture<>();
    try {
      String name = meta.getName();
      appsApi.deleteNamespacedDeploymentAsync(name, kubeNamespace, null, null, null, null, null,
                                              new V1DeleteOptions(), new ApiCallbackAdapter<V1Status>() {
          @Override
          public void onFailure(ApiException e, int statusCode, Map<String, List<String>> responseHeaders) {
            // Ignore the failure if the deployment is already deleted
            if (statusCode == 404) {
              resultFuture.complete(name);
            } else {
              completeExceptionally(resultFuture, e);
            }
          }

          @Override
          public void onSuccess(V1Status v1Status, int statusCode, Map<String, List<String>> responseHeaders) {
            resultFuture.complete(name);
          }
        });
    } catch (ApiException e) {
      completeExceptionally(resultFuture, e);
    }
    return resultFuture;
  }

  /**
   * Deletes the stateful set controlled by this controller asynchronously.

   * @return a {@link CompletionStage} that will complete when the delete operation completed
   */
  private CompletionStage<String> deleteStatefulSet() {
    LOG.debug("Deleting StatefulSet {}", meta.getName());

    AppsV1Api appsApi = new AppsV1Api(apiClient);
    CoreV1Api coreApi = new CoreV1Api(apiClient);

    // callback for the delete sts call
    CompletableFuture<String> resultFuture = new CompletableFuture<>();
    try {
      String name = meta.getName();
      appsApi.deleteNamespacedStatefulSetAsync(name, kubeNamespace, null, null, null, null, null, null,
                                               new ApiCallbackAdapter<V1Status>() {
                                                 @Override
                                                 public void onFailure(ApiException e, int statusCode, Map<String, List<String>> responseHeaders) {
                                                   // Ignore if the stateful set is already deleted
                                                   if (statusCode == 404) {
                                                     deletePVCs();
                                                   } else {
                                                     completeExceptionally(resultFuture, e);
                                                   }
                                                 }

                                                 @Override
                                                 public void onSuccess(V1Status v1Status, int statusCode, Map<String, List<String>> responseHeaders) {
                                                   deletePVCs();
                                                 }

                                                 // Delete the PVCs used by the stateful set
                                                 private void deletePVCs() {
                                                   LOG.debug("Deleting PVCs for StatefulSet {}", meta.getName());
                                                   try {
                                                     coreApi.deleteCollectionNamespacedPersistentVolumeClaimAsync(
                                                       kubeNamespace, null, null, null, null, null, getLabelSelector(),
                                                       null, null, null, null, null, null, null,
                                                       createCallbackFutureAdapter(resultFuture, r -> name));
                                                   } catch (ApiException e) {
                                                     completeExceptionally(resultFuture, e);
                                                   }
                                                 }
                                               });

    } catch (ApiException e) {
      completeExceptionally(resultFuture, e);
    }
    return resultFuture;
  }

  /**
   * Cleans up the resources for deployment and stateful set and returns the future.
   */
  private CompletableFuture<KubeTwillController> cleanupResources() {
    CompletableFuture<KubeTwillController> resultFuture;
    CompletionStage<String> completionStage;
    resultFuture = new CompletableFuture<>();
    // delete the resource
    completionStage = deleteResource();
    completionStage.whenComplete((name, t) -> {
      if (t != null) {
        resultFuture.completeExceptionally(t);
      } else {
        resultFuture.complete(KubeTwillController.this);
      }
    });
    // If the deletion was successful, reflect the status into the completion future
    // If the deletion was failed, don't change the completion state so that caller can retry
    resultFuture.thenAccept(completion::complete);
    return resultFuture;
  }

  /**
   * Cleans up the job and returns future with correct job status.
   */
  private CompletableFuture<KubeTwillController> cleanupJob() {
    // get job status
    CompletionStage<String> completionStage = getJobCompletionStatus();
    completionStage.whenComplete((name, t) -> {
      if (t != null) {
        completion.completeExceptionally(t);
      } else {
        completion.complete(KubeTwillController.this);
      }
    });
    return completion;
  }

  /**
   * Returns job completion status.
   */
  private CompletionStage<String> getJobCompletionStatus() {
    CompletableFuture<String> resultFuture = new CompletableFuture<>();
    String name = meta.getName();
    // In case we fail to get job status due to some transient issue, retry 5 times. There is no need to retry for too
    // long otherwise corresponding program may appear to be stuck while retrying to get job status.
    // If we exhaust retries, complete future exceptionally. Though in normal scenario, retries is not expected
    // to be performed.
    int retryCount = 5;
    do {
      try {
        V1Job v1Job = batchV1Api.readNamespacedJob(name, kubeNamespace, null, null, null);
        V1JobStatus status = v1Job.getStatus();

        // If job has failed, mark future as failed. Else mark it as succeeded.
        if (status != null && status.getFailed() != null) {
          resultFuture.completeExceptionally(new RuntimeException(String.format("Job %s has failed status.", name)));
        } else {
          resultFuture.complete(name);
        }
        retryCount = 0;

        // Also attempt to delete the job once status is available
        deleteJob(meta).whenComplete((jName, t) -> {
          if (t != null) {
            LOG.warn("Failed to delete job {}. Attempt will be retried", jName);
          } else {
            LOG.trace("Successfully deleted job {}", jName);
          }
        });
      } catch (ApiException e) {
        if (e.getCode() == 404) {
          LOG.warn("Failed to get job status because job {} was not found.", name, e);
          // There is no need to retry if job itself is not present. Mark the job as failed.
          retryCount = 0;
          completeExceptionally(resultFuture, e);
        } else {
          retryCount--;
          if (retryCount <= 0) {
            // If we fail to get the job status 5 times, we mark the future as failed.
            LOG.error("Failed to get status for job {}.", name, e);
            completeExceptionally(resultFuture, e);
          }
        }
      }
    } while (retryCount > 0);

    return resultFuture;
  }

  /**
   * Deletes the job.
   */
  private CompletionStage<String> deleteJob(V1ObjectMeta meta) {
    CompletableFuture<String> resultFuture = new CompletableFuture<>();
    String name = meta.getName();
    try {
      V1DeleteOptions v1DeleteOptions = new V1DeleteOptions();
      v1DeleteOptions.setPropagationPolicy("Background");
      // Make async call to attempt to delete job. If it fails, KubeJobCleaner should clean it up.
      batchV1Api.deleteNamespacedJobAsync(name, kubeNamespace, null, null, null, null, null,
                                          v1DeleteOptions, new ApiCallbackAdapter<V1Status>() {
          @Override
          public void onFailure(ApiException e, int statusCode, Map<String, List<String>> responseHeaders) {
            // Ignore the failure if the job is already deleted
            if (statusCode == 404) {
              resultFuture.complete(name);
            } else {
              resultFuture.completeExceptionally(e);
            }
          }

          @Override
          public void onSuccess(V1Status result, int statusCode, Map<String, List<String>> responseHeaders) {
            resultFuture.complete(name);
          }
        });
    } catch (ApiException e) {
      resultFuture.completeExceptionally(e);
    }
    return resultFuture;
  }

  private <T> void completeExceptionally(CompletableFuture<T> future, ApiException e) {
    future.completeExceptionally(new Exception(e.getResponseBody(), e));
  }
}
