/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.submit;


import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.app.runtime.spark.SparkMainWrapper;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import org.apache.spark.deploy.SparkSubmit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Provides common implementation for different {@link SparkSubmitter}.
 */
public abstract class AbstractSparkSubmitter implements SparkSubmitter {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSparkSubmitter.class);

  // Filter for getting archive resources only
  private static final Predicate<LocalizeResource> ARCHIVE_FILTER = new Predicate<LocalizeResource>() {
    @Override
    public boolean apply(LocalizeResource input) {
      return input.isArchive();
    }
  };

  // Transforms LocalizeResource to URI string
  private static final Function<LocalizeResource, String> RESOURCE_TO_PATH = new Function<LocalizeResource, String>() {
    @Override
    public String apply(LocalizeResource input) {
      return input.getURI().toString();
    }
  };

  @Override
  public final <V> ListenableFuture<V> submit(final SparkRuntimeContext runtimeContext,
                                              Map<String, String> configs, List<LocalizeResource> resources,
                                              URI jobFile, final V result) {
    final SparkSpecification spec = runtimeContext.getSparkSpecification();

    final List<String> args = createSubmitArguments(runtimeContext, configs, resources, jobFile);

    // Spark submit is called from this executor
    // Use an executor to simplify logic that is needed to interrupt the running thread on stopping
    final ExecutorService executor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setNameFormat("spark-submitter-" + spec.getName() + "-" + runtimeContext.getRunId())
        .build());

    // Latch for the Spark job completion
    final CountDownLatch completion = new CountDownLatch(1);
    final SparkJobFuture<V> resultFuture = new SparkJobFuture<V>(runtimeContext) {
      @Override
      protected void cancelTask() {
        // Try to shutdown the running spark job.
        triggerShutdown();

        // Wait for the Spark-Submit returns
        Uninterruptibles.awaitUninterruptibly(completion);
      }
    };

    // Submit the Spark job
    executor.submit(new Runnable() {
      @Override
      public void run() {
        List<String> extraArgs = beforeSubmit();
        try {
          String[] submitArgs = Iterables.toArray(Iterables.concat(args, extraArgs), String.class);
          submit(runtimeContext, submitArgs);
          onCompleted(true);
          resultFuture.set(result);
        } catch (Throwable t) {
          onCompleted(false);
          resultFuture.setException(t);
        } finally {
          completion.countDown();
        }
      }
    });
    // Shutdown the executor right after submit since the thread is only used for one submission.
    executor.shutdown();
    return resultFuture;
  }

  /**
   * Add the {@code --master} argument for the Spark submission.
   */
  protected abstract void addMaster(Map<String, String> configs, ImmutableList.Builder<String> argBuilder);

  /**
   * Invoked for stopping the Spark job explicitly.
   */
  protected abstract void triggerShutdown();

  /**
   * Called before submitting the Spark job.
   *
   * @return list of extra arguments to pass to {@link SparkSubmit}.
   */
  protected List<String> beforeSubmit() {
    return Collections.emptyList();
  }

  protected void onCompleted(boolean succeeded) {
    // no-op
  }

  /**
   * Returns configs that are specific to the submission context.
   */
  protected Map<String, String> getSubmitConf() {
    return Collections.emptyMap();
  }

  /**
   * Submits the Spark job using {@link SparkSubmit}.
   *
   * @param runtimeContext context representing the Spark program
   * @param args arguments for the {@link SparkSubmit#main(String[])} method.
   */
  private void submit(SparkRuntimeContext runtimeContext, String[] args) {
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(runtimeContext.getProgramInvocationClassLoader());
    try {
      /*
      Calling SparkSubmit for program:default.simple.-SNAPSHOT.spark.phase-1 8d11cb01-d857-11eb-b681-d6422fc79fa6:
      --master, local[2],
      --conf, spark.app.name=phase-1,
      --conf, spark.executor.memory=2048m,
      --conf, spark.driver.memory=2048m,
      --conf, spark.local.dir=/workDir-9c65f678-e871-4331-b21a-798ab16c76ed/data/tmp/1624915610677-0,
      --conf, spark.driver.cores=1,
      --conf, spark.yarn.maxAppAttempts=1,
      --conf, spark.executor.id=simple,
      --conf, spark.network.timeout=600s,
      --conf, spark.ui.port=0,
      --conf, spark.executor.cores=1,
      --conf, spark.metrics.conf=/workDir-9c65f678-e871-.../data/tmp/1624915610677-0/metrics.properties,
      --conf, spark.speculation=false,
      --conf, spark.sql.caseSensitive=true,
      --conf, spark.app.id=simple,
      --conf, spark.maxRemoteBlockSizeFetchToMem=2147483135,
      --conf, spark.extraListeners=io.cdap.cdap.app.runtime.spark.DelegatingSparkListener,
      --conf, spark.sql.autoBroadcastJoinThreshold=-1,
      --conf, spark.executor.extraJavaOptions=-Dstreaming.checkpoint.rewrite.enabled=true,
      --conf, spark.cdap.localized.resources=["HydratorSpark.config"],
      --conf, spark.driver.extraJavaOptions=-Dstreaming.checkpoint.rewrite.enabled=true,
      --class, io.cdap.cdap.app.runtime.spark.SparkMainWrapper,
      /workDir-9c65f678-e871-4331-b21a-798ab16c76ed/data/runtime/spark/cdapSparkJob.jar,
      --cdap.spark.program=program_run:default.simple.-SNAPSHOT.spark.phase-1.8d11cb01-d857-11eb-b681-d6422fc79fa6,
      --cdap.user.main.class=io.cdap.cdap.etl.spark.batch.BatchSparkPipelineDriver
      LOG.error("ashau - Calling SparkSubmit for {} {}: {}",
                runtimeContext.getProgram().getId(), runtimeContext.getRunId(), Arrays.toString(args));
       */
      LOG.debug("Calling SparkSubmit for {} {}: {}",
                runtimeContext.getProgram().getId(), runtimeContext.getRunId(), Arrays.toString(args));
      // Explicitly set the SPARK_SUBMIT property as it is no longer set on the System properties by the SparkSubmit
      // after the class rewrite. This property only control logging of a warning when submitting the Spark job,
      // hence it's harmless to just leave it there.
      System.setProperty("SPARK_SUBMIT", "true");
      /*
      ./bin/spark-submit --master k8s://https://34.72.68.10
      --deploy-mode cluster
      --name spark-pi
      --class org.apache.spark.examples.SparkPi
      --conf spark.executor.instances=3
       --conf spark.kubernetes.container.image=gcr.io/ashau-dev0/spark:latest
       --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark
       local:///opt/spark/examples/jars/spark-examples_2.12-3.1.2.jar
       */
      String[] dummyArgs = new String[] {
        "--master", "k8s://https://34.72.68.10",
        "--deploy-mode", "cluster",
        "--name", "spark-pi",
        "--class", "org.apache.spark.examples.SparkPi",
        "--conf", "spark.executor.instances=3",
        "--conf", "spark.kubernetes.container.image=gcr.io/ashau-dev0/spark:latest",
        "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
        "--conf", "spark.kubernetes.file.upload.path=gs://ashau-cdap-k8s-test/spark",
        "--files", args[args.length - 3],
        "local:///opt/spark/examples/jars/spark-examples_2.12-3.1.2.jar"
      };
      LOG.error("ashau - Calling SparkSubmit for {} {}: {}",
                runtimeContext.getProgram().getId(), runtimeContext.getRunId(), Arrays.toString(args));
      //SparkSubmit.main(dummyArgs);
      SparkSubmit.main(args);
      LOG.debug("SparkSubmit returned for {} {}", runtimeContext.getProgram().getId(), runtimeContext.getRunId());
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  /**
   * Creates the list of arguments that will be used for calling {@link SparkSubmit#main(String[])}.
   *
   * @param runtimeContext the {@link SparkRuntimeContext} for the spark program
   * @param configs set of Spark configurations
   * @param resources list of resources that needs to be localized to Spark containers
   * @param jobFile the job file for Spark
   * @return a list of arguments
   */
  private List<String> createSubmitArguments(SparkRuntimeContext runtimeContext, Map<String, String> configs,
                                             List<LocalizeResource> resources, URI jobFile) {
    SparkSpecification spec = runtimeContext.getSparkSpecification();

    ImmutableList.Builder<String> builder = ImmutableList.builder();

    addMaster(configs, builder);
    builder.add("--conf").add("spark.app.name=" + spec.getName());

    BiConsumer<String, String> confAdder = (k, v) -> builder.add("--conf").add(k + "=" + v);
    configs.forEach(confAdder);
    getSubmitConf().forEach(confAdder);

    String archives = Joiner.on(',')
      .join(Iterables.transform(Iterables.filter(resources, ARCHIVE_FILTER), RESOURCE_TO_PATH));
    String files = Joiner.on(',')
      .join(Iterables.transform(Iterables.filter(resources, Predicates.not(ARCHIVE_FILTER)), RESOURCE_TO_PATH));

    if (!archives.isEmpty()) {
      builder.add("--archives").add(archives);
    }
    if (!files.isEmpty()) {
      builder.add("--files").add(files);
    }
    // temporary for debugging
    builder.add("--verbose");

    boolean isPySpark = jobFile.getPath().endsWith(".py");
    if (isPySpark) {
      // For python, add extra py library files
      String pyFiles = configs.get("spark.submit.pyFiles");
      if (pyFiles != null) {
        builder.add("--py-files").add(pyFiles);
      }
    } else {
      builder.add("--class").add(SparkMainWrapper.class.getName());
    }

    if ("file".equals(jobFile.getScheme())) {
      builder.add(jobFile.getPath());
    } else {
      builder.add(jobFile.toString());
    }

    if (!isPySpark) {
      // Add extra arguments for easily identifying the program from command line.
      // Arguments to user program is always coming from the runtime arguments.
      builder.add("--cdap.spark.program=" + runtimeContext.getProgramRunId().toString());
      builder.add("--cdap.user.main.class=" + spec.getMainClassName());
    }

    return builder.build();
  }

  /**
   * A {@link Future} implementation for representing a Spark job execution, which allows cancelling the job through
   * the {@link #cancel(boolean)} method. When the job execution is completed, the {@link #set(Object)} should be
   * called for successful execution, or call the {@link #setException(Throwable)} for failure. To terminate the
   * job execution while it is running, call the {@link #cancel(boolean)} method. Sub-classes should override the
   * {@link #cancelTask()} method for cancelling the execution and the state of this {@link Future} will change
   * to cancelled after the {@link #cancelTask()} call returns.
   *
   * @param <V> type of object returned by the {@link #get()} method.
   */
  private abstract static class SparkJobFuture<V> extends AbstractFuture<V> {

    private static final Logger LOG = LoggerFactory.getLogger(SparkJobFuture.class);
    private final AtomicBoolean done;
    private final SparkRuntimeContext context;

    protected SparkJobFuture(SparkRuntimeContext context) {
      this.done = new AtomicBoolean();
      this.context = context;
    }

    @Override
    protected boolean set(V value) {
      if (done.compareAndSet(false, true)) {
        return super.set(value);
      }
      return false;
    }

    @Override
    protected boolean setException(Throwable throwable) {
      if (done.compareAndSet(false, true)) {
        return super.setException(throwable);
      }
      return false;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (!done.compareAndSet(false, true)) {
        return false;
      }

      try {
        cancelTask();
        return super.cancel(mayInterruptIfRunning);
      } catch (Throwable t) {
        // Only log and reset state, but not propagate since Future.cancel() doesn't expect exception to be thrown.
        LOG.warn("Failed to cancel Spark execution for {}.", context, t);
        done.set(false);
        return false;
      }
    }


    @Override
    protected final void interruptTask() {
      // Final it so that it cannot be overridden. This method gets call after the Future state changed
      // to cancel, hence cannot have the caller block until cancellation is done.
    }

    /**
     * Will be called to cancel an executing task. Sub-class can override this method to provide
     * custom cancellation logic. This method will be called before the future changed to cancelled state.
     */
    protected void cancelTask() {
      // no-op
    }
  }
}
