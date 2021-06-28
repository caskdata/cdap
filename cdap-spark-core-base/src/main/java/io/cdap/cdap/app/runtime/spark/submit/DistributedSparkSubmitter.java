/*
 * Copyright © 2017 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextConfig;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeEnv;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeUtils;
import io.cdap.cdap.app.runtime.spark.distributed.SparkExecutionService;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link SparkSubmitter} to submit Spark job that runs on cluster.
 */
public class DistributedSparkSubmitter extends AbstractSparkSubmitter {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedSparkSubmitter.class);

  private final Configuration hConf;
  private final String schedulerQueueName;
  private final SparkExecutionService sparkExecutionService;
  private final long tokenRenewalInterval;

  public DistributedSparkSubmitter(Configuration hConf, LocationFactory locationFactory,
                                   String hostname, SparkRuntimeContext runtimeContext,
                                   @Nullable String schedulerQueueName) {
    this.hConf = hConf;
    this.schedulerQueueName = schedulerQueueName;
    ProgramRunId programRunId = runtimeContext.getProgram().getId().run(runtimeContext.getRunId().getId());
    WorkflowProgramInfo workflowInfo = runtimeContext.getWorkflowInfo();
    BasicWorkflowToken workflowToken = workflowInfo == null ? null : workflowInfo.getWorkflowToken();
    this.sparkExecutionService = new SparkExecutionService(locationFactory, hostname, programRunId, workflowToken);

    Arguments systemArgs = runtimeContext.getProgramOptions().getArguments();
    this.tokenRenewalInterval = systemArgs.hasOption(SparkRuntimeContextConfig.CREDENTIALS_UPDATE_INTERVAL_MS)
      ? Long.parseLong(systemArgs.getOption(SparkRuntimeContextConfig.CREDENTIALS_UPDATE_INTERVAL_MS))
      : -1L;
  }

  @Override
  protected Map<String, String> getSubmitConf() {
    Map<String, String> config = new HashMap<>();
    if (schedulerQueueName != null && !schedulerQueueName.isEmpty()) {
      config.put("spark.yarn.queue", schedulerQueueName);
    }
    if (tokenRenewalInterval > 0) {
      config.put("spark.yarn.token.renewal.interval", Long.toString(tokenRenewalInterval));
    }
    config.put("spark.yarn.appMasterEnv.CDAP_LOG_DIR",  ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    config.put("spark.executorEnv.CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);

    //--conf spark.kubernetes.container.image=gcr.io/ashau-dev0/spark:latest
    //       --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark
    config.put("spark.kubernetes.container.image", "gcr.io/ashau-dev0/spark:latest");
    config.put("spark.kubernetes.container.image.pullPolicy", "Always");
    // this was a service account I manually created
    config.put("spark.kubernetes.authenticate.driver.serviceAccountName", "spark");
    // figure out how to avoid this
    /*
    --archives, file:/workDir-24d8de1a-cc52-4814-9105-2a81bef58db8/data/tmp/1626192451152-0/program.jar.expanded.zip,
    file:/workDir-24d8de1a-cc52-4814-9105-2a81bef58db8/artifacts_archive.jar,
    file:/workDir-24d8de1a-cc52-4814-9105-2a81bef58db8/data/tmp/1626192451152-0/cdap-spark.jar,

    --files,
    file:/workDir-24d8de1a-cc52-4814-9105-2a81bef58db8/tmp/HydratorSpark1122311452461609611.config#HydratorSpark.config,
    file:/workDir-24d8de1a-cc52-4814-9105-2a81bef58db8/data/tmp/1626192451152-0/program.jar,
    file:/workDir-24d8de1a-cc52-4814-9105-2a81bef58db8/data/tmp/1626192451152-0/cdap-spark-launcher.jar,
    file:/workDir-24d8de1a-cc52-4814-9105-2a81bef58db8/metrics.properties,
    file:/workDir-24d8de1a-cc52-4814-9105-2a81bef58db8/data/tmp/1626192451152-0/cConf.xml,
    file:/workDir-24d8de1a-cc52-4814-9105-2a81bef58db8/data/tmp/1626192451152-0/hConf.xml,
    file:/workDir-24d8de1a-cc52-4814-9105-2a81bef58db8/data/tmp/1626192451152-0/logback.xml.jar
     */
    // temporary until figure out how to avoid a hcfs
    config.put("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    config.put("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    config.put("spark.kubernetes.file.upload.path", "gs://ashau-cdap-k8s-test/spark");
    config.put("spark.yarn.security.tokens.hbase.enabled", "false");
    config.put("spark.yarn.security.tokens.hive.enabled", "false");
    config.put("fs.gs.system.bucket", "ashau-cdap-k8s-test");
    config.put("fs.gs.project.id", "ashau-dev0");
    config.put("fs.gs.path.encoding", "uri-path");
    config.put("fs.gs.working.dir", "/");
    config.put("fs.gs.impl.disable.cache", "true");

    return config;
  }

  @Override
  protected void addMaster(Map<String, String> configs, ImmutableList.Builder<String> argBuilder) {
    argBuilder
      //.add("--master").add("yarn")
      // how to get the kubernetes ip? Passed from KubeMasterEnvironment somehow?
      .add("--master").add("k8s://https://34.72.68.10")
      .add("--deploy-mode").add("cluster");
  }

  @Override
  protected List<String> beforeSubmit() {
    // Add all Hadoop configurations to the SparkRuntimeEnv, prefix with "spark.hadoop.". This is
    // how Spark YARN client get hold of Hadoop configurations if those configurations are not in classpath,
    // which is true in CM cluster due to private hadoop conf directory (SPARK-13441) and YARN-4727
    for (Map.Entry<String, String> entry : hConf) {
      SparkRuntimeEnv.setProperty("spark.hadoop." + entry.getKey(), hConf.get(entry.getKey()));
    }

    sparkExecutionService.startAndWait();
    SparkRuntimeEnv.setProperty("spark.yarn.appMasterEnv." + SparkRuntimeUtils.CDAP_SPARK_EXECUTION_SERVICE_URI,
                                sparkExecutionService.getBaseURI().toString());
    return Collections.emptyList();
  }

  @Override
  protected void triggerShutdown() {
    // Just stop the execution service and block on that.
    // It will wait until the "completed" call from the Spark driver.
    sparkExecutionService.stopAndWait();
  }

  @Override
  protected void onCompleted(boolean succeeded) {
    if (succeeded) {
      sparkExecutionService.stopAndWait();
    } else {
      sparkExecutionService.shutdownNow();
    }
  }
}
