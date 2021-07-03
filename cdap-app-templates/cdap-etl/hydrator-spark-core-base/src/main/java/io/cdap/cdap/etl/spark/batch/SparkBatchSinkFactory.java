/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineOutput;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.output.MultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Handles writes to batch sinks. Maintains a mapping from sinks to their outputs and handles serialization and
 * deserialization for those mappings.
 */
public final class SparkBatchSinkFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SparkBatchSinkFactory.class);
  private final Map<String, NamedOutputFormatProvider> outputFormatProviders;
  private final Set<String> uncombinableSinks;
  private final Map<String, DatasetInfo> datasetInfos;
  private final Map<String, Set<String>> sinkOutputs;
  private final Map<String, SQLEngineOutput> sqlOutputs;

  public SparkBatchSinkFactory() {
    this.outputFormatProviders = new HashMap<>();
    this.uncombinableSinks = new HashSet<>();
    this.datasetInfos = new HashMap<>();
    this.sinkOutputs = new HashMap<>();
    this.sqlOutputs = new HashMap<>();
  }

  void addOutput(String stageName, Output output) {
    if (output instanceof Output.DatasetOutput) {
      Output.DatasetOutput datasetOutput = (Output.DatasetOutput) output;
      addOutput(stageName, datasetOutput.getName(), datasetOutput.getAlias(), datasetOutput.getArguments());
      uncombinableSinks.add(stageName);
    } else if (output instanceof Output.OutputFormatProviderOutput) {
      Output.OutputFormatProviderOutput ofpOutput = (Output.OutputFormatProviderOutput) output;
      addOutput(stageName, ofpOutput.getAlias(),
                new NamedOutputFormatProvider(ofpOutput.getName(),
                                              ofpOutput.getOutputFormatProvider().getOutputFormatClassName(),
                                              ofpOutput.getOutputFormatProvider().getOutputFormatConfiguration()));
    } else if (output instanceof SQLEngineOutput) {
      //TODO - separate output from it's contents
      addOutput(stageName, output.getAlias(), (SQLEngineOutput) output);
    } else {
      throw new IllegalArgumentException("Unknown output format type: " + output.getClass().getCanonicalName());
    }
  }

  /**
   * @return stages that use CDAP dataset outputs
   */
  public Set<String> getUncombinableSinks() {
    return uncombinableSinks;
  }

  private void addOutput(String stageName, String alias,
                         NamedOutputFormatProvider outputFormatProvider) {
    if (outputFormatProviders.containsKey(alias) || datasetInfos.containsKey(alias)) {
      throw new IllegalArgumentException("Output already configured: " + alias);
    }
    outputFormatProviders.put(alias, outputFormatProvider);
    addStageOutput(stageName, alias);
  }

  private void addOutput(String stageName, String alias,
                         SQLEngineOutput sqlEngineOutput) {
    if (sqlOutputs.containsKey(stageName)) {
      throw new IllegalArgumentException("Output already configured: " + alias);
    }
    sqlOutputs.put(alias, sqlEngineOutput);
    addStageOutput(stageName, alias);
  }

  private void addOutput(String stageName, String datasetName, String alias, Map<String, String> datasetArgs) {
    if (outputFormatProviders.containsKey(alias) || datasetInfos.containsKey(alias)) {
      throw new IllegalArgumentException("Output already configured: " + alias);
    }
    datasetInfos.put(alias, new DatasetInfo(datasetName, datasetArgs, null));
    addStageOutput(stageName, alias);
  }

  /**
   * Writes a combined RDD using multiple OutputFormatProviders.
   * Returns the set of output names that were written, which still require dataset lineage to be recorded.
   */
  public <K, V> Set<String> writeCombinedRDD(JavaPairRDD<String, KeyValue<K, V>> combinedRDD,
                                             JavaSparkExecutionContext sec, Set<String> sinkNames) {
    Map<String, OutputFormatProvider> outputs = new HashMap<>();
    Set<String> lineageNames = new HashSet<>();
    for (String sinkName : sinkNames) {
      Set<String> sinkOutputNames = sinkOutputs.get(sinkName);
      if (sinkOutputNames == null || sinkOutputNames.isEmpty()) {
        // should never happen if validation happened correctly at pipeline configure time
        throw new IllegalStateException(sinkName + " has no outputs. " +
                                          "Please check that the sink calls addOutput at some point.");
      }

      for (String outputName : sinkOutputNames) {
        NamedOutputFormatProvider outputFormatProvider = outputFormatProviders.get(outputName);
        if (outputFormatProvider == null) {
          // should never happen if planner is implemented correctly and dataset based sinks are not
          // grouped with other sinks
          throw new IllegalStateException(
            String.format("sink '%s' does not use an OutputFormatProvider. " +
                            "This indicates that there is a planner bug. " +
                            "Please report the issue and turn off stage consolidation by setting '%s'" +
                            " to false in the runtime arguments.", sinkName, Constants.CONSOLIDATE_STAGES));
        }
        lineageNames.add(outputFormatProvider.name);
        outputs.put(outputName, outputFormatProvider);
      }
    }

    Configuration hConf = new Configuration();
    Map<String, Set<String>> groupSinkOutputs = new HashMap<>();
    for (String sink : sinkNames) {
      groupSinkOutputs.put(sink, sinkOutputs.get(sink));
    }
    MultiOutputFormat.addOutputs(hConf, outputs, groupSinkOutputs);
    hConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, MultiOutputFormat.class.getName());
    RDDUtils.saveHadoopDataset(combinedRDD, hConf);
    return lineageNames;
  }

  /**
   * Write the given RDD using one or more OutputFormats or CDAP datasets.
   * Returns the names of the outputs written using OutputFormatProvider, which need to register lineage.
   */
  public <K, V> Set<String> writeFromRDD(JavaPairRDD<K, V> rdd, JavaSparkExecutionContext sec, String sinkName) {
    Set<String> outputNames = sinkOutputs.get(sinkName);
    if (outputNames == null || outputNames.isEmpty()) {
      // should never happen if validation happened correctly at pipeline configure time
      throw new IllegalArgumentException(sinkName + " has no outputs. " +
                                           "Please check that the sink calls addOutput at some point.");
    }

    Set<String> lineageNames = new HashSet<>();
    Map<String, OutputFormatProvider> outputFormats = new HashMap<>();
    for (String outputName : outputNames) {
      NamedOutputFormatProvider outputFormatProvider = outputFormatProviders.get(outputName);
      if (outputFormatProvider != null) {
        outputFormats.put(outputName, outputFormatProvider);
        lineageNames.add(outputFormatProvider.name);
      }

      DatasetInfo datasetInfo = datasetInfos.get(outputName);
      if (datasetInfo != null) {
        sec.saveAsDataset(rdd, datasetInfo.getDatasetName(), datasetInfo.getDatasetArgs());
      }
    }

    if (outputFormats.isEmpty()) {
      return lineageNames;
    }

    if (outputFormats.size() == 1) {
      RDDUtils.saveUsingOutputFormat(outputFormats.values().iterator().next(), rdd);
      return lineageNames;
    }

    Configuration hConf = new Configuration();
    Map<String, Set<String>> sinkOutputs = Collections.singletonMap(sinkName, outputFormats.keySet());
    MultiOutputFormat.addOutputs(hConf, outputFormats, sinkOutputs);
    hConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, MultiOutputFormat.class.getName());
    // MultiOutputFormat requires the key to be the sink name and the value to be the actual key-value to
    // send to the delegate output format.
    JavaPairRDD<String, KeyValue<K, V>> multiRDD =
      rdd.mapToPair(kv -> new Tuple2<>(sinkName, new KeyValue<>(kv._1(), kv._2())));
    RDDUtils.saveHadoopDataset(multiRDD, hConf);
    return lineageNames;
  }

  private void addStageOutput(String stageName, String outputName) {
    Set<String> outputs = sinkOutputs.computeIfAbsent(stageName, k -> new HashSet<>());
    outputs.add(outputName);
  }

  public SQLEngineOutput getSQLEngineOutput(String stageName) {
    return !sinkOutputs.containsKey(stageName) ? null :
      sinkOutputs.get(stageName).stream().map(sqlOutputs::get).filter(Objects::nonNull).findFirst().orElse(null);
  }

  static final class NamedOutputFormatProvider implements OutputFormatProvider {
    private final String name;
    private final String outputFormatClassName;
    private final Map<String, String> configuration;

    private NamedOutputFormatProvider(String name, String outputFormatClassName, Map<String, String> configuration) {
      this.name = name;
      this.outputFormatClassName = outputFormatClassName;
      this.configuration = ImmutableMap.copyOf(configuration);
    }

    @Override
    public String getOutputFormatClassName() {
      return outputFormatClassName;
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return configuration;
    }
  }
}
