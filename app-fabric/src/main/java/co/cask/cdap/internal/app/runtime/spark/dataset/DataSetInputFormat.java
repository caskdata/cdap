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

package co.cask.cdap.internal.app.runtime.spark.dataset;

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.internal.app.runtime.spark.BasicSparkContext;
import co.cask.cdap.internal.app.runtime.spark.SparkContextConfig;
import co.cask.cdap.internal.app.runtime.spark.SparkContextProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An {@link InputFormat} that reads from dataset.
 * @param <KEY> Type of key.
 * @param <VALUE> Type of value.
 */
public final class DataSetInputFormat<KEY, VALUE> extends InputFormat<KEY, VALUE> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSetInputFormat.class);
  public static final String HCONF_ATTR_INPUT_DATASET = "input.dataset.name";

  public static void setInput(Job job, String inputDatasetName) {
    job.setInputFormatClass(DataSetInputFormat.class);
    job.getConfiguration().set(DataSetInputFormat.HCONF_ATTR_INPUT_DATASET, inputDatasetName);
  }

  @Override
  public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
    SparkContextConfig sparkContextConfig = new SparkContextConfig(context.getConfiguration());
    List<Split> splits = sparkContextConfig.getInputSelection();
    List<InputSplit> list = new ArrayList<InputSplit>();
    for (Split split : splits) {
      list.add(new DataSetInputSplit(split));
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  @Override
  public RecordReader<KEY, VALUE> createRecordReader(final InputSplit split,
                                                     final TaskAttemptContext context)
    throws IOException, InterruptedException {

    DataSetInputSplit inputSplit = (DataSetInputSplit) split;

    Configuration conf = context.getConfiguration();
    // we don't currently allow datasets as the format between map and reduce stages, otherwise we'll have to
    // pass in the stage here instead of hardcoding mapper.
    SparkContextProvider contextProvider = new SparkContextProvider(context.getConfiguration());
    BasicSparkContext sparkContext = contextProvider.get();
    //sparkContext.getMetricsCollectionService().startAndWait();
    String dataSetName = getInputName(conf);
    BatchReadable<KEY, VALUE> inputDataset = (BatchReadable<KEY, VALUE>) sparkContext.getDataSet(dataSetName);
    SplitReader<KEY, VALUE> splitReader = inputDataset.createSplitReader(inputSplit.getSplit());

    // the record reader now owns the context and will close it
    return new DataSetRecordReader<KEY, VALUE>(splitReader, sparkContext, dataSetName);
  }

  private String getInputName(Configuration conf) {
    return conf.get(HCONF_ATTR_INPUT_DATASET);
  }
}
