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

package io.cdap.cdap.app.runtime.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.spark.util.Utils;
import org.jboss.resteasy.specimpl.ResteasyUriBuilder;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.UriBuilder;

public class SparkSubmitTest {

  @Test
  public void testSparkSubmit() throws IOException {
    List<String> args = new ArrayList<>();

    Map<String, String> sparkConfs = new HashMap<>();
    sparkConfs.put("spark.app.name", "phase-1");
    sparkConfs.put("spark.executor.memory", "2048m");
    sparkConfs.put("spark.driver.memory", "2048m");
    sparkConfs.put("spark.driver.cores", "1");
    sparkConfs.put("spark.executor.cores", "1");
    sparkConfs.put("spark.ui.port", "0");
    sparkConfs.put("spark.metrics.conf", "metrics.properties");
    sparkConfs.put("spark.speculation", "false");
    sparkConfs.put("spark.sql.caseSensitive", "true");
    sparkConfs.put("spark.app.id", "simple");
    sparkConfs.put("spark.maxRemoteBlockSizeFetchToMem", "2147483135");
    sparkConfs.put("spark.extraListeners", "io.cdap.cdap.app.runtime.spark.DelegatingSparkListener");
    sparkConfs.put("spark.sql.autoBroadcastJoinThreshold", "-1");
    sparkConfs.put("spark.executor.extraJavaOptions",
                   "-XX:+UseG1GC -verbose:gc -Xloggc:<LOG_DIR>/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps " +
                     "-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=1M " +
                     "-Dstreaming.checkpoint.rewrite.enabled=true");
    sparkConfs.put("spark.cdap.localized.resources", "[\"HydratorSpark.config\"]");
    sparkConfs.put("spark.driver.extraJavaOptions",
                   "-XX:+UseG1GC -verbose:gc -Xloggc:<LOG_DIR>/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps " +
                     "-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=1M " +
                     "-Dstreaming.checkpoint.rewrite.enabled=true");
    sparkConfs.put("spark.yarn.security.tokens.hbase.enabled", "false");
    sparkConfs.put("spark.yarn.security.tokens.hive.enabled", "false");
    sparkConfs.put("spark.executorEnv.CDAP_LOG_DIR", "<LOG_DIR>");
    sparkConfs.put("spark.yarn.appMasterEnv.CDAP_LOG_DIR", "<LOG_DIR>");

    sparkConfs.put("spark.kubernetes.container.image", "gcr.io/ashau-dev0/spark:latest");
    sparkConfs.put("spark.kubernetes.file.upload.path", "gs://ashau-cdap-k8s-test/spark");
    sparkConfs.put("spark.kubernetes.authenticate.driver.serviceAccountName", "spark");

    Map<String, String> submitConfs = new HashMap<>();
    submitConfs.put("--master", "k8s://https://34.72.68.10");
    submitConfs.put("--deploy-mode", "cluster");
    submitConfs.put("--archives", "file:/workDir-a8b007d4-6dbf-40d4-9f0b-16417429a7f0/data/tmp/1626194665982-0/" +
      "program.jar.expanded.zip");
    //submitConfs.put("--files", "file:/workDir-a8b007d4-6dbf-40d4-9f0b-16417429a7f0/metrics.properties");
    submitConfs.put("--class", "io.cdap.cdap.app.runtime.spark.SparkMainWrapper");

    /*
    UriBuilder.fromUri("file:///workDir-a8b007d4-6dbf-40d4-9f0b-16417429a7f0/data/tmp/1626194665982-0/" +
                         "program.jar.expanded.zip").fragment(null).build();
    ResteasyUriBuilder.fromUri("file:///workDir-a8b007d4-6dbf-40d4-9f0b-16417429a7f0/data/tmp/1626194665982-0/" +
                                 "program.jar.expanded.zip").fragment(null).build();
    ResteasyUriBuilder.fromUri("file:/workDir-a8b007d4-6dbf-40d4-9f0b-16417429a7f0/data/tmp/1626194665982-0/" +
                         "program.jar.expanded.zip").fragment(null).build();
    */

    for (Map.Entry<String, String> conf : sparkConfs.entrySet()) {
      args.add("--conf");
      args.add(String.format("%s=%s", conf.getKey(), conf.getValue()));
    }

    for (Map.Entry<String, String> entry : submitConfs.entrySet()) {
      args.add(entry.getKey());
      args.add(entry.getValue());
    }

    args.add("local:/workDir-a8b007d4-6dbf-40d4-9f0b-16417429a7f0/data/tmp/1626194665982-0/cdapSparkJob.jar");
    args.add("--cdap.spark.program=program_run:default.simple.-SNAPSHOT." +
               "spark.phase-1.955af4a1-e3f9-11eb-bead-6694b2f4aa6d");
    args.add("--cdap.user.main.class=io.cdap.cdap.etl.spark.batch.BatchSparkPipelineDriver");
    boolean loadable = Utils.classIsLoadable("org.apache.spark.deploy.k8s.submit.KubernetesClientApplication");
    URI uri = URI.create("file:/workDir-a8b007d4-6dbf-40d4-9f0b-16417429a7f0/metrics.properties");
    FileSystem fs = FileSystem.get(uri, new Configuration());
    SparkSubmit.main(args.toArray(new String[0]));
  }
}
