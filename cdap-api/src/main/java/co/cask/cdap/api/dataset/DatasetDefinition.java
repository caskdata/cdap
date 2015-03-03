/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.dataset;

import co.cask.cdap.api.annotation.Beta;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Defines named dataset type implementation.
 *
 * The dataset implementation defines:
 * <ul>
 *   <li>
 *     a way to configure new dataset instance by using {@link #configure(String, DatasetProperties)} method
 *   </li>
 *   <li>
 *     a way to perform administrative operations on the dataset instance (e.g. create, drop etc.) by providing
 *     implementation of {@link DatasetAdmin} via
 *     {@link #getAdmin(DatasetContext, ClassLoader, DatasetSpecification)} method
 *   </li>
 *   <li>
 *     a way to perform operations to manipulate data of the dataset instance (e.g. read, write etc.) by providing
 *     implementation of {@link Dataset} via
 *     {@link #getDataset(DatasetContext, Map, ClassLoader, DatasetSpecification)} method
 *   </li>
 * </ul>
 *
 * @param <D> defines data operations that can be performed on this dataset instance
 * @param <A> defines administrative operations that can be performed on this dataset instance
 */
@Beta
public interface DatasetDefinition<D extends Dataset, A extends DatasetAdmin> {

  /**
   * Convenience constant for passing in no arguments.
   */
  public static final Map<String, String> NO_ARGUMENTS = Collections.emptyMap();

  /**
   * @return name of this dataset implementation
   */
  String getName();

  /**
   * Configures new instance of the dataset.
   * @param instanceName name of the instance
   * @param properties instance configuration properties
   * @return instance of {@link DatasetSpecification} that fully describes dataset instance.
   *         The {@link DatasetSpecification} can be used to create {@link DatasetAdmin} and {@link Dataset} to perform
   *         administrative and data operations respectively, see {@link #getAdmin(DatasetContext, ClassLoader,
   *         DatasetSpecification)}
   *         and {@link #getDataset(DatasetContext, Map, ClassLoader, DatasetSpecification)}.
   */
  DatasetSpecification configure(String instanceName, DatasetProperties properties);

  /**
   * Provides dataset admin to be used to perform administrative operations on the dataset instance defined by passed
   * {@link DatasetSpecification}.
   *
   *
   * @param datasetContext context for the dataset
   * @param classLoader classloader to use when executing admin operations
   * @param spec specification of the dataset instance.
   * @return dataset admin to perform administrative operations
   * @throws IOException
   */
  A getAdmin(DatasetContext datasetContext, ClassLoader classLoader, DatasetSpecification spec) throws IOException;

  /**
   * Provides dataset to be used to perform data operations on the dataset instance data defined by passed
   * {@link DatasetSpecification} and the given arguments.
   *
   *
   * @param datasetContext context for the dataset
   * @param arguments arguments for this instance of the dataset. Should not be null - provide an empty map for no
   *                  arguments.
   * @param classLoader classloader to use when executing dataset operations
   * @param spec specification of the dataset instance.
   * @return dataset to perform object operations
   * @throws IOException
   */
  D getDataset(DatasetContext datasetContext, Map<String, String> arguments, ClassLoader classLoader,
               DatasetSpecification spec) throws IOException;
}
