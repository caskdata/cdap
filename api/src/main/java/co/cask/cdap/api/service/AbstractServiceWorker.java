/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.api.service;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Extend this class to add workers to a custom Service.
 */
public abstract class AbstractServiceWorker implements ServiceWorker {

  @Override
  public ServiceWorkerSpecification configure() {
    return new DefaultServiceWorkerSpecification(this, getName(), getDescription(), getRuntimeArguments());
  }

  /**
   * Get the name of the worker. Defaults to the class name.
   *
   * @return the name of this worker
   */
  protected String getName() {
    return getClass().getSimpleName();
  }

  /**
   * Get the description of the worker. Defaults to an empty string.
   *
   * @return the description of this worker, or an empty string if none
   */
  protected String getDescription() {
    return "";
  }

  /**
   * Get the runtime arguments for the worker. Defaults to an empty map.
   *
   * @return the runtime arguments of this worker, or an empty map if none
   */
  protected Map<String, String> getRuntimeArguments() {
    return ImmutableMap.of();
  }

  @Override
  public void initialize(ServiceWorkerContext context) throws Exception {

  }
}
