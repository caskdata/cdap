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

import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.streaming.Windower;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.SparkPairCollection;
import io.cdap.cdap.etl.spark.join.JoinExpressionRequest;
import io.cdap.cdap.etl.spark.join.JoinRequest;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * SQLEngineBackedCollection that wraps another SQLEngineBackedCollection in order to delay the execution of a
 * mapping function.
 *
 * This is currently used to prevent SQL Engine pull operations uniless absolutely needed.
 * @param <T> Type of the wrapped collection records.
 * @param <U> Type of the output collection records.
 */
public class WrappedSQLEngineCollection<T, U> implements SQLBackedCollection<U> {
  private final java.util.function.Function<SparkCollection<T>, SparkCollection<U>> mapper;
  private final SQLBackedCollection<T> wrapped;
  private SparkCollection<U> unwrapped = null;

  public WrappedSQLEngineCollection(SQLBackedCollection<T> wrapped,
                                    java.util.function.Function<SparkCollection<T>, SparkCollection<U>> mapper) {
    this.wrapped = wrapped;
    this.mapper = mapper;
  }
  
  private SparkCollection<U> unwrap() {
    if (unwrapped == null) {
      unwrapped = mapper.apply(wrapped);
    }

    return unwrapped;
  }

  /**
   * Executes an operation on the underlying collection and then wraps it in the same mapper for this collection.
   *
   * This is useful when executing multiple operations in sequence where we need to delegate the operation to the
   * underlying SQL engine and keep delaying the pull operation.
   *
   * By calling this over all wrapped collections, we will eventually reach an instance of a
   * {@link SQLEngineCollection} where the actual operation will take place.
   *
   * @param remapper function used to re-map the underlying collection.
   * @return SQL Backed collection after re-mapping the underlying colleciton and re-adding the mapper.
   */
  private SparkCollection<U> rewrap(
    java.util.function.Function<SparkCollection<T>, SparkCollection<T>> remapper) {
    return new WrappedSQLEngineCollection<>((SQLBackedCollection<T>) remapper.apply(wrapped), mapper);
  }

  @Override
  public <C> C getUnderlying() {
    return unwrap().getUnderlying();
  }

  @Override
  public SparkCollection<U> cache() {
    return unwrap().cache();
  }

  @Override
  public SparkCollection<U> union(SparkCollection<U> other) {
    return unwrap().union(other);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> transform(StageSpec stageSpec, StageStatisticsCollector collector) {
    return unwrap().transform(stageSpec, collector);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> multiOutputTransform(StageSpec stageSpec,
                                                                  StageStatisticsCollector collector) {
    return unwrap().transform(stageSpec, collector);
  }

  @Override
  public <U1> SparkCollection<U1> map(Function<U, U1> function) {
    return unwrap().map(function);
  }

  @Override
  public <U1> SparkCollection<U1> flatMap(StageSpec stageSpec, FlatMapFunction<U, U1> function) {
    return unwrap().flatMap(stageSpec, function);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> aggregate(StageSpec stageSpec,
                                                       @Nullable Integer partitions,
                                                       StageStatisticsCollector collector) {
    return unwrap().aggregate(stageSpec, partitions, collector);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> reduceAggregate(StageSpec stageSpec,
                                                             @Nullable Integer partitions,
                                                             StageStatisticsCollector collector) {
    return unwrap().reduceAggregate(stageSpec, partitions, collector);
  }

  @Override
  public <K, V> SparkPairCollection<K, V> flatMapToPair(PairFlatMapFunction<U, K, V> function) {
    return unwrap().flatMapToPair(function);
  }

  @Override
  public <U1> SparkCollection<U1> compute(StageSpec stageSpec, SparkCompute<U, U1> compute) throws Exception {
    return unwrap().compute(stageSpec, compute);
  }

  @Override
  public Runnable createStoreTask(StageSpec stageSpec,
                                  PairFlatMapFunction<U, Object, Object> sinkFunction) {
    if (wrapped.tryStoreDirect(stageSpec)) {
      return () -> { };
    }
    return unwrap().createStoreTask(stageSpec, sinkFunction);
  }

  @Override
  public boolean tryStoreDirect(StageSpec stageSpec) {
    return wrapped.tryStoreDirect(stageSpec);
  }

  @Override
  public Runnable createMultiStoreTask(PhaseSpec phaseSpec,
                                       Set<String> group,
                                       Set<String> sinks,
                                       Map<String, StageStatisticsCollector> collectors) {
    return unwrap().createMultiStoreTask(phaseSpec, group, sinks, collectors);
  }

  @Override
  public Runnable createStoreTask(StageSpec stageSpec,
                                  SparkSink<U> sink) throws Exception {
    return unwrap().createStoreTask(stageSpec, sink);
  }

  @Override
  public void publishAlerts(StageSpec stageSpec,
                            StageStatisticsCollector collector) throws Exception {
    unwrap().publishAlerts(stageSpec, collector);
  }

  @Override
  public SparkCollection<U> window(StageSpec stageSpec,
                                               Windower windower) {
    return unwrap().window(stageSpec, windower);
  }

  @Override
  public SparkCollection<U> join(JoinRequest joinRequest) {
    return rewrap(c -> c.join(joinRequest));
  }

  @Override
  public SparkCollection<U> join(JoinExpressionRequest joinExpressionRequest) {
    return rewrap(c -> c.join(joinExpressionRequest));
  }
}
