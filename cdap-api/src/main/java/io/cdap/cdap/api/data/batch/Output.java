/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.api.data.batch;

import io.cdap.cdap.api.common.RuntimeArguments;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Defines output of a program, such as MapReduce.
 */
public abstract class Output {

  private final String name;

  private String alias;
  private String namespace;

  protected Output(String name) {
    this.name = name;
  }

  /**
   * @return The name of the output.
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the namespace of the output.
   *
   * @param namespace the namespace of the output
   * @return the Output being operated on
   */
  public Output fromNamespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  /**
   * @return the namespace of the output.
   */
  @Nullable
  public String getNamespace() {
    return namespace;
  }

  /**
   * @return an alias of the output, to be used as the output name instead of the actual name of the
   * output (i.e. dataset name). Defaults to the actual name, in the case that no alias was set.
   */
  public String getAlias() {
    return alias == null ? name : alias;
  }

  /**
   * Sets an alias to be used as the output name.
   *
   * @param alias the alias to be set for this Output
   * @return the Output being operated on
   */
  public Output alias(String alias) {
    this.alias = alias;
    return this;
  }

  /**
   * Returns an Output defined by a dataset.
   *
   * @param datasetName the name of the output dataset
   */
  public static Output ofDataset(String datasetName) {
    return ofDataset(datasetName, RuntimeArguments.NO_ARGUMENTS);
  }

  /**
   * Returns an Output defined by a dataset.
   *  @param datasetName the name of the output dataset
   * @param arguments the arguments to use when instantiating the dataset
   */
  public static Output ofDataset(String datasetName, Map<String, String> arguments) {
    return new DatasetOutput(datasetName, arguments);
  }

  /**
   * Returns an Output defined by an OutputFormatProvider.
   *
   * @param outputName the name of the output
   * @param outputFormatProvider an instance of an OutputFormatProvider. It can not be an instance of
   *                             a {@link DatasetOutputCommitter}.
   */
  public static Output of(String outputName, OutputFormatProvider outputFormatProvider) {
    return new OutputFormatProviderOutput(outputName, outputFormatProvider);
  }

  /**
   * An implementation of {@link Output}, which defines a {@link io.cdap.cdap.api.dataset.Dataset} as an output.
   */
  public static class DatasetOutput extends Output {

    private final Map<String, String> arguments;

    private DatasetOutput(String name, Map<String, String> arguments) {
      super(name);
      this.arguments = Collections.unmodifiableMap(new HashMap<>(arguments));
    }

    private DatasetOutput(String name, Map<String, String> arguments, String namespace) {
      this(name, arguments);
      super.fromNamespace(namespace);
    }

    public Map<String, String> getArguments() {
      return arguments;
    }

    @Override
    public DatasetOutput fromNamespace(String namespace) {
      return new DatasetOutput(super.name, arguments, namespace);
    }
  }

  /**
   * An implementation of {@link Output}, which defines an {@link OutputFormatProvider} as an output.
   */
  public static class OutputFormatProviderOutput extends Output {

    private final OutputFormatProvider outputFormatProvider;

    private OutputFormatProviderOutput(String name, OutputFormatProvider outputFormatProvider) {
      super(name);
      this.outputFormatProvider = outputFormatProvider;
    }

    public OutputFormatProvider getOutputFormatProvider() {
      return outputFormatProvider;
    }

    @Override
    public Output fromNamespace(String namespace) {
      throw new UnsupportedOperationException("OutputFormatProviderOutput does not support setting namespace.");
    }
  }
}
