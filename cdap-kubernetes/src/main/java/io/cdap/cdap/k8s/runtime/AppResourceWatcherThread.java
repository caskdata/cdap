/*
 * Copyright © 2020 Cask Data, Inc.
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

import io.cdap.cdap.k8s.common.AbstractWatcherThread;
import io.cdap.cdap.k8s.common.ResourceChangeListener;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.util.Config;
import okhttp3.Call;
import org.apache.twill.common.Cancellable;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A thread for monitoring Kubernetes application resource state change.
 *
 * @param <T> type of Kubernetes resource for which state changes to be monitored
 */
abstract class AppResourceWatcherThread<T> extends AbstractWatcherThread<T> {

  /**
   * Creates a {@link AppResourceWatcherThread} for watching {@link V1Deployment} events.
   */
  static AppResourceWatcherThread<V1Deployment> createDeploymentWatcher(String namespace, String selector) {
    return new AppResourceWatcherThread<V1Deployment>("apps", "v1", "deployments", namespace, selector) { };
  }

  /**
   * Creates a {@link AppResourceWatcherThread} for watching {@link V1StatefulSet} events.
   */
  static AppResourceWatcherThread<V1StatefulSet> createStatefulSetWatcher(String namespace, String selector) {
    return new AppResourceWatcherThread<V1StatefulSet>("apps", "v1", "statefulsets", namespace, selector) { };
  }

  /**
   * Creates a {@link AppResourceWatcherThread} for watching {@link V1Job} events.
   */
  static AppResourceWatcherThread<V1Job> createJobWatcher(String namespace, String selector) {
    return new AppResourceWatcherThread<V1Job>("batch", "v1", "jobs", namespace, selector) { };
  }

  private final String group;
  private final String version;
  private final String plural;
  private final String selector;
  private final Queue<ResourceChangeListener<T>> listeners;
  private volatile ApiClient apiClient;

  private AppResourceWatcherThread(String group, String version, String plural, String namespace, String selector) {
    super("kube-" + plural + "-watch", namespace);
    setDaemon(true);
    this.group = group;
    this.version = version;
    this.plural = plural;
    this.selector = selector;
    this.listeners = new ConcurrentLinkedQueue<>();
  }

  Cancellable addListener(ResourceChangeListener<T> listener) {
    // Wrap the listener for removal
    ResourceChangeListener<T> wrappedListener = wrapListener(listener);
    listeners.add(wrappedListener);
    resetWatch();
    return () -> listeners.remove(wrappedListener);
  }

  @Override
  protected Call createCall(String namespace, @Nullable String labelSelector) throws IOException, ApiException {
    CustomObjectsApi api = new CustomObjectsApi(getApiClient());
    return api.listNamespacedCustomObjectCall(group, version, namespace, plural, null, null, null,
                                              labelSelector, null, null, null, true, null);
  }

  @Nullable
  @Override
  protected String getSelector() {
    return selector;
  }

  @Override
  public void resourceAdded(T resource) {
    listeners.forEach(l -> l.resourceAdded(resource));
  }

  @Override
  public void resourceModified(T resource) {
    listeners.forEach(l -> l.resourceModified(resource));
  }

  @Override
  public void resourceDeleted(T resource) {
    listeners.forEach(l -> l.resourceDeleted(resource));
  }

  @Override
  protected ApiClient getApiClient() throws IOException {
    ApiClient client = apiClient;
    if (client != null) {
      return client;
    }

    synchronized (this) {
      client = apiClient;
      if (client != null) {
        return client;
      }

      client = Config.defaultClient();

      // Set a reasonable timeout for the watch.
      client.setReadTimeout((int) TimeUnit.MINUTES.toMillis(5));
      apiClient = client;
      return client;
    }
  }

  private ResourceChangeListener<T> wrapListener(ResourceChangeListener<T> listener) {
    return new ResourceChangeListener<T>() {

      @Override
      public void resourceAdded(T resource) {
        listener.resourceAdded(resource);
      }

      @Override
      public void resourceModified(T resource) {
        listener.resourceModified(resource);
      }

      @Override
      public void resourceDeleted(T resource) {
        listener.resourceDeleted(resource);
      }
    };
  }
}
