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

import com.google.common.base.Objects;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.engine.sql.SQLEngine;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineOutput;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPullDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPushDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinDefinition;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLWriteRequest;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultStageMetrics;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.engine.SQLEngineJob;
import io.cdap.cdap.etl.engine.SQLEngineJobKey;
import io.cdap.cdap.etl.engine.SQLEngineJobType;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.function.TransformFromPairFunction;
import io.cdap.cdap.etl.spark.function.TransformToPairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;

/**
 * Adapter used to orchestrate interaction between the Pipeline Runner and the SQL Engine.
 *
 * @param <T> type for records supported by this BatchSQLEngineAdapter.
 */
public class BatchSQLEngineAdapter<T> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BatchSQLEngineAdapter.class);

  private final JavaSparkExecutionContext sec;
  private final SQLEngine<?, ?, ?, ?> sqlEngine;
  private final Metrics metrics;
  private final Map<String, StageStatisticsCollector> statsCollectors;
  private final ExecutorService executorService =
    Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("batch-sql-engine-adapter"));
  private final Map<SQLEngineJobKey, SQLEngineJob<?>> jobs;

  public BatchSQLEngineAdapter(SQLEngine<?, ?, ?, ?> sqlEngine,
                               JavaSparkExecutionContext sec,
                               Map<String, StageStatisticsCollector> statsCollectors) {
    this.sqlEngine = sqlEngine;
    this.sec = sec;
    this.metrics = sec.getMetrics();
    this.statsCollectors = statsCollectors;
    this.jobs = new HashMap<>();
  }

  /**
   * Call the SQLEngine PrepareRun method
   * @throws Exception if the underlying prepareRun call fails.
   */
  public void prepareRun() throws Exception {
    sqlEngine.prepareRun(sec);
  }

  /**
   * Call the SQLEngine onRunFinish method
   * @throws Exception if the underlying onRunFinish call fails.
   */
  public void onRunFinish(boolean succeeded) throws Exception {
    sqlEngine.onRunFinish(succeeded, sec);
  }

  /**
   * Creates a new job tu push a SparkCollection into the SQL engine.
   *
   * @param datasetName the name of the dataset to push
   * @param schema      the schema for this dataset
   * @param collection  the Spark collection containing the dataset to push
   * @return Job representing this Push operation.
   */
  @SuppressWarnings("unchecked,raw")
  protected SQLEngineJob<SQLDataset> push(String datasetName,
                                          Schema schema,
                                          SparkCollection<?> collection) {
    //If this job already exists, return the existing instance.
    SQLEngineJobKey jobKey = new SQLEngineJobKey(datasetName, SQLEngineJobType.PUSH);
    if (jobs.containsKey(jobKey)) {
      return (SQLEngineJob<SQLDataset>) jobs.get(jobKey);
    }

    CompletableFuture<SQLDataset> future = new CompletableFuture<>();

    Runnable pushTask = () -> {
      try {
        LOG.debug("Starting push for dataset '{}'", datasetName);
        SQLDataset result = pushInternal(datasetName, schema, collection);
        LOG.debug("Completed push for dataset '{}'", datasetName);
        future.complete(result);
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    };

    executorService.submit(pushTask);

    SQLEngineJob<SQLDataset> job = new SQLEngineJob<>(jobKey, future);
    jobs.put(jobKey, job);

    return job;
  }

  /**
   * Push implementation. This method has blocking calls and should be executed in a separate thread.
   *
   * @param datasetName name of the dataset to push.
   * @param schema      the record schema.
   * @param collection  the collection containing the records to push.
   * @return {@link SQLDataset} instance representing the pushed records.
   * @throws SQLEngineException if the push operation fails.
   */
  public SQLDataset pushInternal(String datasetName,
                                 Schema schema,
                                 SparkCollection<?> collection) throws SQLEngineException {
    // Create push request
    SQLPushRequest pushRequest = new SQLPushRequest(datasetName, schema);

    //Get the push provider and wait for it to be ready to use
    SQLPushDataset<StructuredRecord, ?, ?> pushDataset = sqlEngine.getPushProvider(pushRequest);

    //Write records using the Push provider.
    JavaPairRDD<?, ?> pairRdd =
      ((JavaRDD) collection.getUnderlying()).flatMapToPair(new TransformToPairFunction<>(pushDataset.toKeyValue()));
    RDDUtils.saveUsingOutputFormat(pushDataset, pairRdd);
    return pushDataset;
  }

  /**
   * Creates a new job to pull a Spark Collection from the SQL engine
   *
   * @param job the job representing the compute stage for the dataset we need to pull.
   * @param jsc the Java Spark context instance.
   * @return Job representing this pull operation.
   */
  @SuppressWarnings("unchecked,raw")
  public SQLEngineJob<JavaRDD<T>> pull(SQLEngineJob<SQLDataset> job,
                                       JavaSparkContext jsc) {
    //If this job already exists, return the existing instance.
    SQLEngineJobKey jobKey = new SQLEngineJobKey(job.getDatasetName(), SQLEngineJobType.PULL);
    if (jobs.containsKey(jobKey)) {
      return (SQLEngineJob<JavaRDD<T>>) jobs.get(jobKey);
    }

    CompletableFuture<JavaRDD<T>> future = new CompletableFuture<>();

    Runnable pullTask = () -> {
      try {
        LOG.debug("Starting pull for dataset '{}'", job.getDatasetName());
        waitForJobAndHandleExceptionInternal(job);
        JavaRDD<T> result = pullInternal(job.waitFor(), jsc);
        LOG.debug("Completed pull for dataset '{}'", job.getDatasetName());
        future.complete(result);
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    };

    executorService.submit(pullTask);

    SQLEngineJob<JavaRDD<T>> pullJob = new SQLEngineJob<>(jobKey, future);
    jobs.put(jobKey, pullJob);

    return pullJob;
  }

  /**
   * Pull implementation. This method has blocking calls and should be executed in a separate thread.
   *
   * @param dataset the dataset to pull.
   * @param jsc the Java Spark Context instance.
   * @return {@link JavaRDD} representing the records contained in this dataset.
   * @throws SQLEngineException if the pull process fails.
   */
  @SuppressWarnings("unchecked,raw")
  private JavaRDD<T> pullInternal(SQLDataset dataset,
                                  JavaSparkContext jsc) throws SQLEngineException {
    // Create pull operation for this dataset and wait until completion
    SQLPullRequest pullRequest = new SQLPullRequest(dataset);
    SQLPullDataset<StructuredRecord, ?, ?> sqlPullDataset = sqlEngine.getPullProvider(pullRequest);

    // Run operation to read from the InputFormatProvider supplied by this operation.
    ClassLoader classLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                   getClass().getClassLoader());
    JavaPairRDD pairRDD = RDDUtils.readUsingInputFormat(jsc, sqlPullDataset, classLoader, Object.class,
                                                        Object.class);
    return pairRDD.flatMap(new TransformFromPairFunction(sqlPullDataset.fromKeyValue()));
  }

  /**
   * Check if a collection exists in the SQL Engine.
   * <p>
   * If there are already jobs in place to either push or compute this dataset, we will assume that this job exists on
   * the engine.
   * <p>
   * Otherwise, we delegate to the SQL engine.
   *
   * @param datasetName the name of the dataset to verify
   * @return boolean detailing if the collection exists or not.
   */
  public boolean exists(String datasetName) {
    SQLEngineJobKey joinStagePushKey = new SQLEngineJobKey(datasetName, SQLEngineJobType.PUSH);
    if (jobs.containsKey(joinStagePushKey)) {
      return true;
    }

    SQLEngineJobKey joinStageExecKey = new SQLEngineJobKey(datasetName, SQLEngineJobType.EXECUTE);
    if (jobs.containsKey(joinStageExecKey)) {
      return true;
    }

    return false;
  }

  /**
   * Verify if a Join Definition can be executed on a SQL Engine.
   *
   * @param datasetName    the dataset name to use to store the result of the join operation
   * @param joinDefinition the Join Definition
   * @return boolean specifying if this join operation can be executed on the SQL engine.
   */
  public boolean canJoin(String datasetName,
                         JoinDefinition joinDefinition) {
    SQLJoinDefinition sqlJoinDefinition = new SQLJoinDefinition(datasetName, joinDefinition);
    return sqlEngine.canJoin(sqlJoinDefinition);
  }

  /**
   * Executes a Join operation in the SQL engine
   *
   * @param datasetName    the dataset name to use to store the result of the join operation
   * @param joinDefinition the Join Definition
   * @return Job representing this join operation
   */
  @SuppressWarnings("unchecked,raw")
  public SQLEngineJob<SQLDataset> join(String datasetName,
                                       JoinDefinition joinDefinition) {
    //If this job already exists, return the existing instance.
    SQLEngineJobKey jobKey = new SQLEngineJobKey(datasetName, SQLEngineJobType.EXECUTE);
    if (jobs.containsKey(jobKey)) {
      return (SQLEngineJob<SQLDataset>) jobs.get(jobKey);
    }

    CompletableFuture<SQLDataset> future = new CompletableFuture<>();

    Runnable joinTask = () -> {
      try {
        LOG.debug("Starting join for dataset '{}'", datasetName);
        Collection<SQLDataset> inputDatasets = getJoinInputDatasets(joinDefinition);
        SQLJoinRequest joinRequest = new SQLJoinRequest(datasetName, joinDefinition, inputDatasets);

        if (!sqlEngine.canJoin(joinRequest)) {
          throw new IllegalArgumentException("Unable to execute this join in the SQL engine");
        }

        joinInternal(future, joinRequest);
        LOG.debug("Completed join for dataset '{}'", datasetName);
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    };

    executorService.submit(joinTask);

    SQLEngineJob<SQLDataset> job = new SQLEngineJob<>(jobKey, future);
    jobs.put(jobKey, job);

    return job;
  }

  /**
   * Gets all datasets that are required to execute the supplied Join Definition.
   *
   * @param joinDefinition the join definition to check
   * @return Collection containing all the {@link SQLDataset} instances needed to execute this join.
   */
  @SuppressWarnings("unchecked")
  private Collection<SQLDataset> getJoinInputDatasets(JoinDefinition joinDefinition) throws IllegalArgumentException {
    List<SQLDataset> datasets = new ArrayList<>(joinDefinition.getStages().size());

    for (JoinStage stage : joinDefinition.getStages()) {
      // Wait for the previous push or execute jobs to complete
      SQLEngineJobKey pushJobKey = new SQLEngineJobKey(stage.getStageName(), SQLEngineJobType.PUSH);
      SQLEngineJobKey execJobKey = new SQLEngineJobKey(stage.getStageName(), SQLEngineJobType.EXECUTE);

      if (jobs.containsKey(pushJobKey)) {
        SQLEngineJob<SQLDataset> job = (SQLEngineJob<SQLDataset>) jobs.get(pushJobKey);
        waitForJobAndHandleExceptionInternal(job);
        datasets.add(job.waitFor());
      } else if (jobs.containsKey(execJobKey)) {
        SQLEngineJob<SQLDataset> job = (SQLEngineJob<SQLDataset>) jobs.get(execJobKey);
        waitForJobAndHandleExceptionInternal(job);
        datasets.add(job.waitFor());
      } else {
        throw new IllegalArgumentException("No SQL Engine job exists for stage " + stage.getStageName());
      }
    }

    return datasets;
  }

  /**
   * Join implementation. This method has blocking calls and should be executed in a separate thread.
   *
   * @param future the future instance to use to return results.
   * @param joinRequest the Join Request
   * @throws SQLEngineException   if any of the preceding jobs fails.
   */
  private void joinInternal(CompletableFuture<SQLDataset> future,
                            SQLJoinRequest joinRequest)
    throws SQLEngineException {

    String datasetName = joinRequest.getDatasetName();
    DefaultStageMetrics stageMetrics = new DefaultStageMetrics(metrics, datasetName);
    StageStatisticsCollector statisticsCollector = statsCollectors.get(datasetName);

    // Count input metrics for each of the preceding stages.
    for (SQLDataset inputDataset : joinRequest.getInputDatasets()) {
      countRecordsIn(inputDataset, statisticsCollector, stageMetrics);
    }

    //Execute Join job.
    SQLDataset joinDataset = (SQLDataset) sqlEngine.join(joinRequest);

    // Count output rows and complete future.
    countRecordsOut(joinDataset, statisticsCollector, stageMetrics);
    future.complete(joinDataset);
  }

  /**
   * Stops all jobs from executing and cleans up the SQL engine.
   */
  @Override
  public void close() throws RuntimeException {
    RuntimeException ex = null;

    Set<String> datasetNames = new HashSet<>();
    // Stop all jobs
    for (SQLEngineJob<?> job : jobs.values()) {
      try {
        datasetNames.add(job.getDatasetName());
        job.cancel();
      } catch (Throwable t) {
        if (ex == null) {
          ex = new RuntimeException(t);
        } else {
          ex.addSuppressed(t);
        }
      }
    }
    // Cleanup all datasets from the SQL engine.
    for (String datasetName : datasetNames) {
      try {
        sqlEngine.cleanup(datasetName);
      } catch (SQLEngineException t) {
        if (ex == null) {
          ex = t;
        } else {
          ex.addSuppressed(t);
        }
      }
    }
    // Stop the executor service
    executorService.shutdown();

    if (ex != null) {
      throw ex;
    }
  }

  /**
   * Method used to handle execution exceptions from a job.
   *
   * Any error during execution will be wrapped in a SQL exception if needed, and the SQL Engine Adapter will be shut
   * down.
   *
   * @param job the job to wait for.
   * @throws SQLEngineException if the internal task threw an exception.
   */
  private void waitForJobAndHandleExceptionInternal(SQLEngineJob<?> job) throws SQLEngineException {
    SQLEngineException ex = null;

    try {
      job.waitFor();
    } catch (CompletionException ce) {
      LOG.error("SQL Engine Task completed exceptionally");
      try {
        throw ce.getCause();
      } catch (SQLEngineException see) {
        // If the source of this was an SQL exception, just rethrow.
        LOG.error("SQL Engine Task failed with exception.", see);
        ex = see;
      } catch (Throwable t) {
        // Wrap any other exception in SQL exception
        LOG.error("SQL Engine Task failed with exception.", t);
        ex = new SQLEngineException(t);
      }
    } catch (CancellationException ce) {
      LOG.error("SQL Engine Task was cancelled", ce);
      ex = new SQLEngineException(ce);
    } catch (Throwable t) {
      LOG.error("SQL Engine Task failed with unexpected throwable", t);
      ex = new SQLEngineException(t);
    }
    // Throw SQL Exception if needed.
    if (ex != null) {
      throw ex;
    }
  }

  /**
   * Block until an SQL Engine job is completed.
   *
   * If an exception is thrown by this job, this stops the SQL engine and executed cleanup operations.
   *
   * @param job the job to wait for completion
   * @throws SQLEngineException exception thrown when this job was stopped.
   */
  public void waitForJobAndHandleException(SQLEngineJob<?> job) throws SQLEngineException {
    try {
      waitForJobAndHandleExceptionInternal(job);
    } catch (SQLEngineException e) {
      // If an exception is thrown, stop this SQL engine adapter (including all tasks) and cleanup existing datasets
      // from the SQL engine.
      try {
        close();
      } catch (Throwable t) {
        e.addSuppressed(t);
      }
      throw e;
    }
  }

  /**
   * Method to aggregate input metrics.
   *
   * @param dataset                  the dataset to use to count metrics
   * @param stageStatisticsCollector Stage Statistics Collector for this stage
   * @param stageMetrics             Metrics for this stage
   */
  private void countRecordsIn(SQLDataset dataset,
                              @Nullable StageStatisticsCollector stageStatisticsCollector,
                              StageMetrics stageMetrics) {
    // Count input metrics
    if (stageStatisticsCollector != null) {
      stageStatisticsCollector.incrementInputRecordCount(dataset.getNumRows());
    }
    stageMetrics.count(Constants.Metrics.RECORDS_IN, (int) dataset.getNumRows());
  }

  /**
   * Method to aggregate output metrics.
   *
   * @param dataset                  the dataset to use to count metrics
   * @param stageStatisticsCollector Stage Statistics Collector for this stage
   * @param stageMetrics             Metrics for this stage
   */
  private void countRecordsOut(SQLDataset dataset,
                               @Nullable StageStatisticsCollector stageStatisticsCollector,
                               StageMetrics stageMetrics) {
    // Count input metrics
    if (stageStatisticsCollector != null) {
      stageStatisticsCollector.incrementOutputRecordCount(dataset.getNumRows());
    }
    stageMetrics.count(Constants.Metrics.RECORDS_OUT, (int) dataset.getNumRows());
  }

  public boolean write(String datasetName, SQLEngineOutput sqlEngineOutput) {
    SQLEngineJobKey execJobKey = new SQLEngineJobKey(datasetName, SQLEngineJobType.EXECUTE);

    if (jobs.containsKey(execJobKey)) {
      LOG.debug("Waiting for dataset {} to be ready", datasetName);
      jobs.get(execJobKey).waitFor();
      LOG.debug("Attempting write for dataset {} into {}", datasetName, sqlEngineOutput);
      boolean wrote = sqlEngine.write(new SQLWriteRequest(datasetName, sqlEngineOutput));
      LOG.debug("Write dataset {} into {} was {}", datasetName, sqlEngineOutput, wrote ? "completed" : "refused");
      return wrote;
    } else {
      LOG.warn("Could not find join result job for {}, skipping direct write", datasetName);
      return false;
    }
  }
}
