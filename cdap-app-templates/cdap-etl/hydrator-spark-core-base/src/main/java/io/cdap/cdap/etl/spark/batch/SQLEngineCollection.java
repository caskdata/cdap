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

package io.cdap.cdap.etl.spark.batch;

import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineOutput;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.streaming.Windower;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.engine.SQLEngineJob;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.SparkPairCollection;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import io.cdap.cdap.etl.spark.join.JoinExpressionRequest;
import io.cdap.cdap.etl.spark.join.JoinRequest;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SQLContext;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Spark Collection representing records stored in a SQL engine.
 *
 * Records will be pulled into Spark and operations delegated to an RDDCollection as needed.
 * @param <T> type of records stored in this {@link SparkCollection}.
 */
public class SQLEngineCollection<T> implements SQLBackedCollection<T> {
  private final JavaSparkExecutionContext sec;
  private final JavaSparkContext jsc;
  private final SQLContext sqlContext;
  private final DatasetContext datasetContext;
  private final SparkBatchSinkFactory sinkFactory;
  private final FunctionCache.Factory functionCacheFactory;
  private final String datasetName;
  private final BatchSQLEngineAdapter<T> adapter;
  private final SQLEngineJob<SQLDataset> job;
  private SparkCollection<T> localCollection;

  public SQLEngineCollection(JavaSparkExecutionContext sec,
                             FunctionCache.Factory functionCacheFactory,
                             JavaSparkContext jsc,
                             SQLContext sqlContext,
                             DatasetContext datasetContext,
                             SparkBatchSinkFactory sinkFactory,
                             SparkCollection<T> localCollection,
                             String datasetName,
                             BatchSQLEngineAdapter adapter,
                             SQLEngineJob<SQLDataset> job) {
    this.sec = sec;
    this.jsc = jsc;
    this.sqlContext = sqlContext;
    this.datasetContext = datasetContext;
    this.sinkFactory = sinkFactory;
    this.functionCacheFactory = functionCacheFactory;
    this.datasetName = datasetName;
    this.adapter = adapter;
    this.job = job;
    this.localCollection = localCollection;
  }

  public SQLEngineCollection(JavaSparkExecutionContext sec,
                             FunctionCache.Factory functionCacheFactory,
                             JavaSparkContext jsc,
                             SQLContext sqlContext,
                             DatasetContext datasetContext,
                             SparkBatchSinkFactory sinkFactory,
                             String datasetName,
                             BatchSQLEngineAdapter adapter,
                             SQLEngineJob<SQLDataset> job) {
    this.sec = sec;
    this.jsc = jsc;
    this.sqlContext = sqlContext;
    this.datasetContext = datasetContext;
    this.sinkFactory = sinkFactory;
    this.functionCacheFactory = functionCacheFactory;
    this.datasetName = datasetName;
    this.adapter = adapter;
    this.job = job;
    this.localCollection = null;
  }

  /**
   * If an operation needs to be executed outside of the scope of a SQL Engine, we will need to pull this SQL
   * collection into an RDDCollection and delegate the operation to the RDD collection.
   * @return (@link RDDCollection} representing the records pulled from the SQL Engine.
   */
  @SuppressWarnings("raw")
  private SparkCollection<T> pull() {
    if (localCollection == null) {
      SQLEngineJob<JavaRDD<T>> pullJob = adapter.pull(job, jsc);
      adapter.waitForJobAndHandleException(pullJob);
      JavaRDD<T> rdd = pullJob.waitFor();
      localCollection =
        new RDDCollection<>(sec, functionCacheFactory, jsc, sqlContext, datasetContext, sinkFactory, rdd);
    }
    return localCollection;
  }

  @Override
  public <C> C getUnderlying() {
    return (C) pull().getUnderlying();
  }

  @Override
  public SparkCollection<T> cache() {
    return pull().cache();
  }

  @Override
  public SparkCollection<T> union(SparkCollection<T> other) {
    return pull().union(other);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> transform(StageSpec stageSpec, StageStatisticsCollector collector) {
    return pull().transform(stageSpec, collector);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> multiOutputTransform(StageSpec stageSpec,
                                                                  StageStatisticsCollector collector) {
    return pull().transform(stageSpec, collector);
  }

  @Override
  public <U> SparkCollection<U> map(Function<T, U> function) {
    return pull().map(function);
  }

  @Override
  public <U> SparkCollection<U> flatMap(StageSpec stageSpec, FlatMapFunction<T, U> function) {
    return pull().flatMap(stageSpec, function);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> aggregate(StageSpec stageSpec,
                                                       @Nullable Integer partitions,
                                                       StageStatisticsCollector collector) {
    return pull().aggregate(stageSpec, partitions, collector);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> reduceAggregate(StageSpec stageSpec,
                                                             @Nullable Integer partitions,
                                                             StageStatisticsCollector collector) {
    return pull().reduceAggregate(stageSpec, partitions, collector);
  }

  @Override
  public <K, V> SparkPairCollection<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function) {
    return pull().flatMapToPair(function);
  }

  @Override
  public <U> SparkCollection<U> compute(StageSpec stageSpec, SparkCompute<T, U> compute) throws Exception {
    return pull().compute(stageSpec, compute);
  }

  public boolean tryStoreDirect(StageSpec stageSpec) {
    SQLEngineOutput sqlEngineOutput = sinkFactory.getSQLEngineOutput(stageSpec.getName());
    if (sqlEngineOutput != null) {
      //Try writing directly
      return adapter.write(datasetName, sqlEngineOutput);
    }
    return false;
  }

  @Override
  public Runnable createStoreTask(StageSpec stageSpec, PairFlatMapFunction<T, Object, Object> sinkFunction) {
    return pull().createStoreTask(stageSpec, sinkFunction);
  }

  @Override
  public Runnable createMultiStoreTask(PhaseSpec phaseSpec, Set<String> group, Set<String> sinks,
                                       Map<String, StageStatisticsCollector> collectors) {
    return pull().createMultiStoreTask(phaseSpec, group, sinks, collectors);
  }

  @Override
  public Runnable createStoreTask(StageSpec stageSpec, SparkSink<T> sink) throws Exception {
    return pull().createStoreTask(stageSpec, sink);
  }

  @Override
  public void publishAlerts(StageSpec stageSpec, StageStatisticsCollector collector) throws Exception {
    pull().publishAlerts(stageSpec, collector);
  }

  @Override
  public SparkCollection<T> window(StageSpec stageSpec, Windower windower) {
    return pull().window(stageSpec, windower);
  }

  @Override
  public SparkCollection<T> join(JoinRequest joinRequest) {
    String joinStageName = joinRequest.getStageName();
    SQLEngineJob<SQLDataset> job = adapter.join(joinStageName, joinRequest.getJoinDefinition());
    return new SQLEngineCollection<>(sec, functionCacheFactory, jsc, sqlContext, datasetContext, sinkFactory,
                                     joinStageName, adapter, job);
  }

  @Override
  public SparkCollection<T> join(JoinExpressionRequest joinRequest) {
    String joinStageName = joinRequest.getStageName();
    SQLEngineJob<SQLDataset> job = adapter.join(joinStageName, joinRequest.getJoinDefinition());
    return new SQLEngineCollection<>(sec, functionCacheFactory, jsc, sqlContext, datasetContext, sinkFactory,
                                     joinStageName, adapter, job);
  }
}
