/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.test.app;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.AppUsingNamespace;
import io.cdap.cdap.ConfigTestApp;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.api.common.Scope;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetArguments;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.table.Get;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricSearchQuery;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.api.metrics.RuntimeMetrics;
import io.cdap.cdap.api.metrics.TagValue;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.DefaultId;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.WorkflowTokenDetail;
import io.cdap.cdap.proto.WorkflowTokenNodeDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.MapReduceManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.SlowTests;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkerManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.cdap.test.XSlowTests;
import io.cdap.cdap.test.artifacts.AppWithPlugin;
import io.cdap.cdap.test.artifacts.plugins.ToStringPlugin;
import io.cdap.cdap.test.base.TestFrameworkTestBase;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.filesystem.Location;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 *
 */
@Category(SlowTests.class)
public class TestFrameworkTestRun extends TestFrameworkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestFrameworkTestRun.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_METADATASCOPE_METADATA_TYPE =
    new TypeToken<Map<MetadataScope, Metadata>>() { }.getType();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.CLUSTER_NAME, "testCluster");
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private final NamespaceId testSpace = new NamespaceId("testspace");

  @Before
  public void setUp() throws Exception {
    getNamespaceAdmin().create(new NamespaceMeta.Builder().setName(testSpace).build());
  }

  @Test
  public void testOrgHamcrest() {
    // this tests that org.hamcrest classes can be used in tests without class loading conflicts (see CDAP-7391)
    Assert.assertThat("oh hello yeah", CoreMatchers.containsString("hello"));
  }

  @Test
  public void testInvalidAppWithData() throws Exception {
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("invalid-app", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, AppWithDuplicateData.class);
    ArtifactId pluginArtifactId = NamespaceId.DEFAULT.artifact("test-plugin", "1.0.0-SNAPSHOT");
    addPluginArtifact(pluginArtifactId, artifactId, ToStringPlugin.class);

    ApplicationId appId = NamespaceId.DEFAULT.app("InvalidApp");

    // test with duplicate dataset should fail as we don't allow multiple deployment of dataset
    try {
      AppRequest<AppWithDuplicateData.ConfigClass> createRequest = new AppRequest<>(
        new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
        new AppWithDuplicateData.ConfigClass(true, false, false));
      deployApplication(appId, createRequest);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }

    // test with duplicate module should fail as we don't allow multiple deployment of modules
    try {
      AppRequest<AppWithDuplicateData.ConfigClass> createRequest = new AppRequest<>(
        new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
        new AppWithDuplicateData.ConfigClass(false, false, true));
      deployApplication(appId, createRequest);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }

    // test with duplicate plugin should pass as we allow multiple deployment of same plugin
    AppRequest<AppWithDuplicateData.ConfigClass> createRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
      new AppWithDuplicateData.ConfigClass(false, true, false));
    deployApplication(appId, createRequest);

    // test with no duplicates should pass
    createRequest = new AppRequest<>(new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
                                     new AppWithDuplicateData.ConfigClass(false, false, false));
    deployApplication(appId, createRequest);
  }

  @Test
  public void testWorkerThrowingException() throws Exception {
    ApplicationManager appManager = deployApplication(AppWithExceptionThrowingWorker.class);
    final WorkerManager workerManager = appManager.getWorkerManager(AppWithExceptionThrowingWorker.WORKER_NAME);

    // Only one instance of the worker and it throws an exception.
    // ProgramRunStatus should go to FAILED state, except the one that throws in the destroy() method.
    testExceptionWorker(workerManager, 0, 0);

    // Test a case where worker completes without an exception.
    // There should be in total two completed runs.
    // One from the one that throws in destroy() in testExceptionWorker(), one from the next line.
    workerManager.start();
    workerManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 5, TimeUnit.SECONDS);

    // Few of the instances of the worker will throw an exception, while others complete normally. Still the
    // ProgramRunStatus should go to FAILED state, , except for those that throws in the destroy() method.
    workerManager.setInstances(9);
    testExceptionWorker(workerManager, 2, 2);

    // Test a case where worker completes without an exception.
    // There should be four completed runs.
    // Two from throws that throws in destroy() in testExceptionWorker(),
    // one from the previous normal run, and one from next line.
    workerManager.start();
    workerManager.waitForRuns(ProgramRunStatus.COMPLETED, 4, 10, TimeUnit.SECONDS);
  }

  private void testExceptionWorker(WorkerManager workerManager, int completedCountSoFar,
                                   int failedCountSoFar) throws Exception {
    workerManager.start(ImmutableMap.of(AppWithExceptionThrowingWorker.INITIALIZE, ""));
    workerManager.waitForRuns(ProgramRunStatus.FAILED, 1 + failedCountSoFar, 3, TimeUnit.SECONDS);
    workerManager.start(ImmutableMap.of(AppWithExceptionThrowingWorker.RUN, ""));
    workerManager.waitForRuns(ProgramRunStatus.FAILED, 2 + failedCountSoFar, 3, TimeUnit.SECONDS);

    // Throwing exception on the destroy() method won't fail the program execution.
    workerManager.start(ImmutableMap.of(AppWithExceptionThrowingWorker.DESTROY, ""));
    workerManager.waitForRuns(ProgramRunStatus.COMPLETED, 1 + completedCountSoFar, 3, TimeUnit.SECONDS);
  }

  @Test
  public void testServiceManager() throws Exception {
    ApplicationManager applicationManager = deployApplication(AppWithServices.class);
    final ServiceManager serviceManager = applicationManager.getServiceManager(AppWithServices.SERVICE_NAME);
    serviceManager.setInstances(2);
    Assert.assertEquals(0, serviceManager.getProvisionedInstances());
    Assert.assertEquals(2, serviceManager.getRequestedInstances());
    Assert.assertFalse(serviceManager.isRunning());

    List<RunRecord> history = serviceManager.getHistory();
    Assert.assertEquals(0, history.size());

    serviceManager.start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    Assert.assertEquals(2, serviceManager.getProvisionedInstances());

    // requesting with ProgramRunStatus.KILLED returns empty list
    history = serviceManager.getHistory(ProgramRunStatus.KILLED);
    Assert.assertEquals(0, history.size());

    // requesting with either RUNNING or ALL will return one record
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return serviceManager.getHistory(ProgramRunStatus.RUNNING).size();
      }
    }, 5, TimeUnit.SECONDS);

    history = serviceManager.getHistory(ProgramRunStatus.RUNNING);
    Assert.assertEquals(ProgramRunStatus.RUNNING, history.get(0).getStatus());

    history = serviceManager.getHistory(ProgramRunStatus.ALL);
    Assert.assertEquals(1, history.size());
    Assert.assertEquals(ProgramRunStatus.RUNNING, history.get(0).getStatus());
  }

  @Test
  public void testNamespaceAvailableAtRuntime() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, AppUsingNamespace.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(AppUsingNamespace.SERVICE_NAME);
    serviceManager.start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    URL serviceURL = serviceManager.getServiceURL(10, TimeUnit.SECONDS);
    Assert.assertEquals(testSpace.getNamespace(), callServiceGet(serviceURL, "ns"));

    serviceManager.stop();
    serviceManager.waitForStopped(10, TimeUnit.SECONDS);
  }

  @Test
  public void testAppConfigWithNull() throws Exception {
    testAppConfig(ConfigTestApp.NAME, deployApplication(ConfigTestApp.class), null);
  }

  @Test
  public void testAppConfig() throws Exception {
    ConfigTestApp.ConfigClass conf = new ConfigTestApp.ConfigClass("testDataset");
    testAppConfig(ConfigTestApp.NAME, deployApplication(ConfigTestApp.class, conf), conf);
  }

  @Test
  public void testAppWithPlugin() throws Exception {
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("app-with-plugin", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, AppWithPlugin.class);

    ArtifactId pluginArtifactId = NamespaceId.DEFAULT.artifact("test-plugin", "1.0.0-SNAPSHOT");
    addPluginArtifact(pluginArtifactId, artifactId, ToStringPlugin.class);

    ApplicationId appId = NamespaceId.DEFAULT.app("AppWithPlugin");
    AppRequest createRequest = new AppRequest(new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()));

    ApplicationManager appManager = deployApplication(appId, createRequest);

    final WorkerManager workerManager = appManager.getWorkerManager(AppWithPlugin.WORKER);
    workerManager.start();
    workerManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.SECONDS);

    final ServiceManager serviceManager = appManager.getServiceManager(AppWithPlugin.SERVICE);
    serviceManager.start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    URL serviceURL = serviceManager.getServiceURL(5, TimeUnit.SECONDS);
    callServiceGet(serviceURL, "dummy");
    serviceManager.stop();
    serviceManager.waitForStopped(10, TimeUnit.SECONDS);

    WorkflowManager workflowManager = appManager.getWorkflowManager(AppWithPlugin.WORKFLOW);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    List<RunRecord> runRecords = workflowManager.getHistory();
    Assert.assertNotEquals(ProgramRunStatus.FAILED, runRecords.get(0).getStatus());
    DataSetManager<KeyValueTable> workflowTableManager = getDataset(AppWithPlugin.WORKFLOW_TABLE);
    String value = Bytes.toString(workflowTableManager.get().read("val"));
    Assert.assertEquals(AppWithPlugin.TEST, value);
    Map<String, String> workflowTags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE,
                                                       NamespaceId.DEFAULT.getNamespace(),
                                                       Constants.Metrics.Tag.APP,
                                                       "AppWithPlugin",
                                                       Constants.Metrics.Tag.WORKFLOW,
                                                       AppWithPlugin.WORKFLOW,
                                                       Constants.Metrics.Tag.RUN_ID,
                                                       runRecords.get(0).getPid()
                                                       );

    getMetricsManager().waitForTotalMetricCount(workflowTags,
                                                String.format("user.destroy.%s", AppWithPlugin.WORKFLOW),
                                                1, 60, TimeUnit.SECONDS);

    // Testing Spark Plugins. First send some data to fileset for the Spark program to process
    DataSetManager<FileSet> fileSetManager = getDataset(AppWithPlugin.SPARK_INPUT);
    FileSet fileSet = fileSetManager.get();
    try (PrintStream out = new PrintStream(fileSet.getLocation("input").append("file.txt").getOutputStream(),
                                           true, "UTF-8")) {
      for (int i = 0; i < 5; i++) {
        out.println("Message " + i);
      }
    }

    Map<String, String> sparkArgs = new HashMap<>();
    FileSetArguments.setInputPath(sparkArgs, "input");

    SparkManager sparkManager = appManager.getSparkManager(AppWithPlugin.SPARK).start(sparkArgs);
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

    // Verify the Spark result.
    DataSetManager<Table> dataSetManager = getDataset(AppWithPlugin.SPARK_TABLE);
    Table table = dataSetManager.get();
    try (Scanner scanner = table.scan(null, null)) {
      for (int i = 0; i < 5; i++) {
        Row row = scanner.next();
        Assert.assertNotNull(row);
        String expected = "Message " + i + " " + AppWithPlugin.TEST;
        Assert.assertEquals(expected, Bytes.toString(row.getRow()));
        Assert.assertEquals(expected, Bytes.toString(row.get(expected)));
      }
      // There shouldn't be any more rows in the table.
      Assert.assertNull(scanner.next());
    }
  }

  @Test
  public void testAppFromArtifact() throws Exception {
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("cfg-app", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);

    ApplicationId appId = NamespaceId.DEFAULT.app("AppFromArtifact");
    AppRequest<ConfigTestApp.ConfigClass> createRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
      new ConfigTestApp.ConfigClass("testStream", "testDataset")
    );
    ApplicationManager appManager = deployApplication(appId, createRequest);
    testAppConfig(appId.getApplication(), appManager, createRequest.getConfig());
  }

  @Test
  public void testAppVersionsCreation() throws Exception {
    ArtifactId artifactId = new ArtifactId(NamespaceId.DEFAULT.getNamespace(), "cfg-app", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);

    ApplicationId appId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), "AppV1", "version1");
    AppRequest<ConfigTestApp.ConfigClass> createRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
      new ConfigTestApp.ConfigClass("tD1", "tV1"));
    ApplicationManager appManager = deployApplication(appId, createRequest);
    ServiceManager serviceManager = appManager.getServiceManager(ConfigTestApp.SERVICE_NAME);
    serviceManager.start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    URL serviceURL = serviceManager.getServiceURL();
    Gson gson = new Gson();
    Assert.assertEquals("tV1", gson.fromJson(callServiceGet(serviceURL, "ping"), String.class));
    serviceManager.stop();
    serviceManager.waitForStopped(10, TimeUnit.SECONDS);

    appId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), "AppV1", "version2");
    createRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
      new ConfigTestApp.ConfigClass("tD2", "tV2"));
    appManager = deployApplication(appId, createRequest);
    serviceManager = appManager.getServiceManager(ConfigTestApp.SERVICE_NAME);
    serviceManager.start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    serviceURL = serviceManager.getServiceURL();
    Assert.assertEquals("tV2", gson.fromJson(callServiceGet(serviceURL, "ping"), String.class));
    serviceManager.stop();
  }

  private void testAppConfig(String appName, ApplicationManager appManager,
                             ConfigTestApp.ConfigClass conf) throws Exception {
    String datasetName = conf == null ? ConfigTestApp.DEFAULT_TABLE : conf.getTableName();

    ServiceManager serviceManager = appManager.getServiceManager(ConfigTestApp.SERVICE_NAME).start();
    URL serviceURL = serviceManager.getServiceURL(5, TimeUnit.SECONDS);

    // Write data to the table using the service
    URL url = new URL(serviceURL, "write/abcd");
    Assert.assertEquals(200, executeHttp(HttpRequest.put(url).build()).getResponseCode());
    url = new URL(serviceURL, "write/xyz");
    Assert.assertEquals(200, executeHttp(HttpRequest.put(url).build()).getResponseCode());

    DataSetManager<KeyValueTable> dsManager = getDataset(datasetName);
    KeyValueTable table = dsManager.get();
    Assert.assertEquals("abcd", Bytes.toString(table.read(appName + ".abcd")));
    Assert.assertEquals("xyz", Bytes.toString(table.read(appName + ".xyz")));
  }

  @Category(SlowTests.class)
  @Test
  public void testMapperDatasetAccess() throws Exception {
    addDatasetInstance("keyValueTable", "table1");
    addDatasetInstance("keyValueTable", "table2");
    DataSetManager<KeyValueTable> tableManager = getDataset("table1");
    KeyValueTable inputTable = tableManager.get();
    inputTable.write("hello", "world");
    tableManager.flush();

    ApplicationManager appManager = deployApplication(DatasetWithMRApp.class);
    Map<String, String> argsForMR =
      ImmutableMap.of(DatasetWithMRApp.INPUT_KEY, "table1",
                      DatasetWithMRApp.OUTPUT_KEY, "table2",
                      "task.*." + SystemArguments.METRICS_CONTEXT_TASK_INCLUDED, "false");
    MapReduceManager mrManager = appManager.getMapReduceManager(DatasetWithMRApp.MAPREDUCE_PROGRAM).start(argsForMR);
    mrManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    appManager.stopAll();

    DataSetManager<KeyValueTable> outTableManager = getDataset("table2");
    verifyMapperJobOutput(DatasetWithMRApp.class, outTableManager);
    // test that the metrics emitted by MR task is collected
    testTaskMetric(mrManager.getHistory().get(0).getPid(), true);
    // test that the metrics is not emitted with instance tag(task-level)
    testTaskTagLevelExists(DatasetWithMRApp.class.getSimpleName(),
                           DatasetWithMRApp.MAPREDUCE_PROGRAM,
                           mrManager.getHistory().get(0).getPid(), "table1", false);
  }

  @Category(SlowTests.class)
  @Test
  public void testMapReduceTaskMetricsDisable() throws Exception {
    addDatasetInstance("keyValueTable", "table1");
    addDatasetInstance("keyValueTable", "table2");
    DataSetManager<KeyValueTable> tableManager = getDataset("table1");
    KeyValueTable inputTable = tableManager.get();
    inputTable.write("hello", "world");
    tableManager.flush();

    ApplicationManager appManager = deployApplication(DatasetWithMRApp.class);
    Map<String, String> argsForMR =
      ImmutableMap.of(DatasetWithMRApp.INPUT_KEY, "table1",
                      DatasetWithMRApp.OUTPUT_KEY, "table2",
                      "task.*." + SystemArguments.METRICS_ENABLED, "false");
    MapReduceManager mrManager = appManager.getMapReduceManager(DatasetWithMRApp.MAPREDUCE_PROGRAM).start(argsForMR);
    mrManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    appManager.stopAll();

    testTaskMetric(mrManager.getHistory().get(0).getPid(), false);
  }

  private void testTaskTagLevelExists(String appName,
                                      String programName, String runId,
                                      String datasetName, boolean doesExist) throws Exception {
    List<TagValue> tags = new ArrayList<>();
    tags.add(new TagValue(Constants.Metrics.Tag.NAMESPACE, NamespaceId.DEFAULT.getNamespace()));
    tags.add(new TagValue(Constants.Metrics.Tag.APP, appName));
    tags.add(new TagValue(Constants.Metrics.Tag.MAPREDUCE, programName));
    tags.add(new TagValue(Constants.Metrics.Tag.RUN_ID, runId));
    tags.add(new TagValue(Constants.Metrics.Tag.DATASET, datasetName));
    tags.add(new TagValue(Constants.Metrics.Tag.MR_TASK_TYPE, "m"));
    Collection<TagValue> tagsValues =
      getMetricsManager().searchTags(new MetricSearchQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE, tags));
    Assert.assertEquals(doesExist, !tagsValues.isEmpty());
    if (doesExist) {
      Assert.assertEquals(Constants.Metrics.Tag.INSTANCE_ID, tagsValues.iterator().next().getName());
    }
  }

  private void testTaskMetric(String runId, boolean doesExist) throws Exception {
    List<TagValue> tags = new ArrayList<>();
    tags.add(new TagValue(Constants.Metrics.Tag.NAMESPACE, NamespaceId.DEFAULT.getNamespace()));
    tags.add(new TagValue(Constants.Metrics.Tag.APP, DatasetWithMRApp.class.getSimpleName()));
    tags.add(new TagValue(Constants.Metrics.Tag.MAPREDUCE, DatasetWithMRApp.MAPREDUCE_PROGRAM));
    tags.add(new TagValue(Constants.Metrics.Tag.RUN_ID, runId));

    Collection<String> metricNames =
      getMetricsManager().searchMetricNames(new MetricSearchQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE, tags));
    // we disabled task level metrics; this should return empty list
    Assert.assertEquals(doesExist, metricNames.contains("user.test.metric"));
  }

  @Category(SlowTests.class)
  @Test
  public void testCrossNSMapperDatasetAccess() throws Exception {
    NamespaceMeta inputNS = new NamespaceMeta.Builder().setName("inputNS").build();
    NamespaceMeta outputNS = new NamespaceMeta.Builder().setName("outputNS").build();
    getNamespaceAdmin().create(inputNS);
    getNamespaceAdmin().create(outputNS);

    addDatasetInstance(inputNS.getNamespaceId().dataset("table1"), "keyValueTable");
    addDatasetInstance(outputNS.getNamespaceId().dataset("table2"), "keyValueTable");
    DataSetManager<KeyValueTable> tableManager = getDataset(inputNS.getNamespaceId().dataset("table1"));
    KeyValueTable inputTable = tableManager.get();
    inputTable.write("hello", "world");
    tableManager.flush();

    ApplicationManager appManager = deployApplication(DatasetCrossNSAccessWithMAPApp.class);
    Map<String, String> argsForMR = ImmutableMap.of(
      DatasetCrossNSAccessWithMAPApp.INPUT_DATASET_NS, inputNS.getName(),
      DatasetCrossNSAccessWithMAPApp.INPUT_DATASET_NAME, "table1",
      DatasetCrossNSAccessWithMAPApp.OUTPUT_DATASET_NS, outputNS.getName(),
      DatasetCrossNSAccessWithMAPApp.OUTPUT_DATASET_NAME, "table2");
    MapReduceManager mrManager = appManager.getMapReduceManager(DatasetCrossNSAccessWithMAPApp.MAPREDUCE_PROGRAM)
      .start(argsForMR);
    mrManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    appManager.stopAll();

    DataSetManager<KeyValueTable> outTableManager = getDataset(outputNS.getNamespaceId().dataset("table2"));
    verifyMapperJobOutput(DatasetCrossNSAccessWithMAPApp.class, outTableManager);
  }

  private void verifyMapperJobOutput(Class<?> appClass,
                                     DataSetManager<KeyValueTable> outTableManager) throws Exception {
    KeyValueTable outputTable = outTableManager.get();
    Assert.assertEquals("world", Bytes.toString(outputTable.read("hello")));

    // Verify dataset metrics
    String readCountName = "system." + Constants.Metrics.Name.Dataset.READ_COUNT;
    String writeCountName = "system." + Constants.Metrics.Name.Dataset.WRITE_COUNT;
    Collection<MetricTimeSeries> metrics = getMetricsManager().query(
      new MetricDataQuery(
        0, System.currentTimeMillis() / 1000, Integer.MAX_VALUE,
        ImmutableMap.of(
          readCountName, AggregationFunction.SUM,
          writeCountName, AggregationFunction.SUM
        ),
        ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, DefaultId.NAMESPACE.getNamespace(),
                        Constants.Metrics.Tag.APP, appClass.getSimpleName(),
                        Constants.Metrics.Tag.MAPREDUCE, DatasetWithMRApp.MAPREDUCE_PROGRAM),
        ImmutableList.<String>of()
      )
    );

    // Transform the collection of metrics into a map from metrics name to aggregated sum
    Map<String, Long> aggs = Maps.transformEntries(Maps.uniqueIndex(metrics, new Function<MetricTimeSeries, String>() {
      @Override
      public String apply(MetricTimeSeries input) {
        return input.getMetricName();
      }
    }), new Maps.EntryTransformer<String, MetricTimeSeries, Long>() {
      @Override
      public Long transformEntry(String key, MetricTimeSeries value) {
        Preconditions.checkArgument(value.getTimeValues().size() == 1,
                                    "Expected one value for aggregated sum for metrics %s", key);
        return value.getTimeValues().get(0).getValue();
      }
    });

    Assert.assertEquals(Long.valueOf(1), aggs.get(readCountName));
    Assert.assertEquals(Long.valueOf(1), aggs.get(writeCountName));
  }

  @Test
  public void testWorkflowStatus() throws Exception {
    ApplicationManager appManager = deployApplication(WorkflowStatusTestApp.class);

    File workflowSuccess = new File(TMP_FOLDER.newFolder() + "/workflow.success");
    File actionSuccess = new File(TMP_FOLDER.newFolder() + "/action.success");
    File workflowKilled = new File(TMP_FOLDER.newFolder() + "/workflow.killed");
    File firstFile = new File(TMP_FOLDER.newFolder() + "/first");
    File firstFileDone = new File(TMP_FOLDER.newFolder() + "/first.done");

    WorkflowManager workflowManager = appManager.getWorkflowManager(WorkflowStatusTestApp.WORKFLOW_NAME);
    workflowManager.start(ImmutableMap.of("workflow.success.file", workflowSuccess.getAbsolutePath(),
                                          "action.success.file", actionSuccess.getAbsolutePath(),
                                          "throw.exception", "true"));
    workflowManager.waitForRun(ProgramRunStatus.FAILED, 1, TimeUnit.MINUTES);

    // Since action and workflow failed the files should not exist
    Assert.assertFalse(workflowSuccess.exists());
    Assert.assertFalse(actionSuccess.exists());

    workflowManager.start(ImmutableMap.of("workflow.success.file", workflowSuccess.getAbsolutePath(),
                                          "action.success.file", actionSuccess.getAbsolutePath()));
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);
    Assert.assertTrue(workflowSuccess.exists());
    Assert.assertTrue(actionSuccess.exists());

    // Test the killed status
    workflowManager.start(ImmutableMap.of("workflow.killed.file", workflowKilled.getAbsolutePath(),
                                          "first.file", firstFile.getAbsolutePath(),
                                          "first.done.file", firstFileDone.getAbsolutePath(),
                                          "test.killed", "true"));
    verifyFileExists(Lists.newArrayList(firstFile));
    workflowManager.stop();
    workflowManager.waitForRun(ProgramRunStatus.KILLED, 1, TimeUnit.MINUTES);
    Assert.assertTrue(workflowKilled.exists());
  }

  @Category(SlowTests.class)
  @Test
  public void testCustomActionDatasetAccess() throws Exception {
    addDatasetInstance("keyValueTable", DatasetWithCustomActionApp.CUSTOM_TABLE);
    addDatasetInstance("fileSet", DatasetWithCustomActionApp.CUSTOM_FILESET);

    ApplicationManager appManager = deployApplication(DatasetWithCustomActionApp.class);
    ServiceManager serviceManager = appManager.getServiceManager(DatasetWithCustomActionApp.CUSTOM_SERVICE).start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    WorkflowManager workflowManager = appManager.getWorkflowManager(DatasetWithCustomActionApp.CUSTOM_WORKFLOW).start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);
    appManager.stopAll();

    DataSetManager<KeyValueTable> outTableManager = getDataset(DatasetWithCustomActionApp.CUSTOM_TABLE);
    KeyValueTable outputTable = outTableManager.get();

    Assert.assertEquals("world", Bytes.toString(outputTable.read("hello")));
    Assert.assertEquals("service", Bytes.toString(outputTable.read("hi")));
    Assert.assertEquals("another.world", Bytes.toString(outputTable.read("another.hello")));

    DataSetManager<FileSet> outFileSetManager = getDataset(DatasetWithCustomActionApp.CUSTOM_FILESET);
    FileSet fs = outFileSetManager.get();
    try (InputStream in = fs.getLocation("test").getInputStream()) {
      Assert.assertEquals(42, in.read());
    }
  }

  @Category(XSlowTests.class)
  @Test
  public void testWorkflowLocalDatasets() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, WorkflowAppWithLocalDatasets.class);

    // Execute Workflow without keeping the local datasets after run
    Map<String, String> additionalParams = new HashMap<>();
    String runId = executeWorkflow(applicationManager, additionalParams, 1);
    verifyWorkflowRun(runId, false, false, "COMPLETED");

    additionalParams.put("dataset.wordcount.keep.local", "true");
    runId = executeWorkflow(applicationManager, additionalParams, 2);
    verifyWorkflowRun(runId, true, false, "COMPLETED");

    additionalParams.clear();
    additionalParams.put("dataset.*.keep.local", "true");
    runId = executeWorkflow(applicationManager, additionalParams, 3);
    verifyWorkflowRun(runId, true, true, "COMPLETED");

    additionalParams.clear();
    additionalParams.put("dataset.*.keep.local", "true");
    additionalParams.put("destroy.throw.exception", "true");
    runId = executeWorkflow(applicationManager, additionalParams, 4);
    verifyWorkflowRun(runId, true, true, "STARTED");

    WorkflowManager wfManager = applicationManager.getWorkflowManager(WorkflowAppWithLocalDatasets.WORKFLOW_NAME);
    List<RunRecord> history = wfManager.getHistory();
    Assert.assertEquals(4, history.size());
    for (RunRecord record : history) {
      Assert.assertEquals(ProgramRunStatus.COMPLETED, record.getStatus());
    }
  }

  private void verifyFileExists(final List<File> fileList)
    throws Exception {
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        for (File file : fileList) {
          if (!file.exists()) {
            return false;
          }
        }
        return true;
      }
    }, 100, TimeUnit.SECONDS);
  }

  private void verifyWorkflowRun(String runId, boolean shouldKeepWordCountDataset, boolean shouldKeepCSVFilesetDataset,
                                 String expectedRunStatus)
    throws Exception {

    // Once the Workflow run is complete local datasets should not be available
    DataSetManager<KeyValueTable> localKeyValueDataset =
      getDataset(testSpace.dataset(WorkflowAppWithLocalDatasets.WORDCOUNT_DATASET + "." + runId));

    if (shouldKeepWordCountDataset) {
      Assert.assertNotNull(localKeyValueDataset.get());
    } else {
      Assert.assertNull(localKeyValueDataset.get());
    }

    DataSetManager<FileSet> localFileSetDataset =
      getDataset(testSpace.dataset(WorkflowAppWithLocalDatasets.CSV_FILESET_DATASET + "." + runId));

    if (shouldKeepCSVFilesetDataset) {
      Assert.assertNotNull(localFileSetDataset.get());
    } else {
      Assert.assertNull(localFileSetDataset.get());
    }

    // Dataset which is not local should still be available
    DataSetManager<KeyValueTable> nonLocalKeyValueDataset =
      getDataset(testSpace.dataset(WorkflowAppWithLocalDatasets.RESULT_DATASET));
    Assert.assertEquals("6", Bytes.toString(nonLocalKeyValueDataset.get().read("UniqueWordCount")));

    // There should not be any local copy of the non local dataset
    nonLocalKeyValueDataset = getDataset(testSpace.dataset(WorkflowAppWithLocalDatasets.RESULT_DATASET + "." + runId));
    Assert.assertNull(nonLocalKeyValueDataset.get());

    DataSetManager<KeyValueTable> workflowRuns
      = getDataset(testSpace.dataset(WorkflowAppWithLocalDatasets.WORKFLOW_RUNS_DATASET));

    Assert.assertEquals(expectedRunStatus, Bytes.toString(workflowRuns.get().read(runId)));
  }

  private String executeWorkflow(ApplicationManager applicationManager, Map<String, String> additionalParams,
                                 int expectedComplete) throws Exception {
    WorkflowManager wfManager = applicationManager.getWorkflowManager(WorkflowAppWithLocalDatasets.WORKFLOW_NAME);
    Map<String, String> runtimeArgs = new HashMap<>();
    File waitFile = new File(TMP_FOLDER.newFolder(), "/wait.file");
    File doneFile = new File(TMP_FOLDER.newFolder(), "/done.file");

    runtimeArgs.put("input.path", "input");
    runtimeArgs.put("output.path", "output");
    runtimeArgs.put("wait.file", waitFile.getAbsolutePath());
    runtimeArgs.put("done.file", doneFile.getAbsolutePath());
    runtimeArgs.putAll(additionalParams);
    wfManager.start(runtimeArgs);

    // Wait until custom action in the Workflow is triggered.
    while (!waitFile.exists()) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Now the Workflow should have RUNNING status. Get its runid.
    List<RunRecord> history = wfManager.getHistory(ProgramRunStatus.RUNNING);
    Assert.assertEquals(1, history.size());
    String runId = history.get(0).getPid();

    // Get the local datasets for this Workflow run
    DataSetManager<KeyValueTable> localDataset =
      getDataset(testSpace.dataset(WorkflowAppWithLocalDatasets.WORDCOUNT_DATASET + "." + runId));
    Assert.assertEquals("2", Bytes.toString(localDataset.get().read("text")));

    DataSetManager<FileSet> fileSetDataset =
      getDataset(testSpace.dataset(WorkflowAppWithLocalDatasets.CSV_FILESET_DATASET + "." + runId));
    Assert.assertNotNull(fileSetDataset.get());

    // Local datasets should not exist at the namespace level
    localDataset = getDataset(testSpace.dataset(WorkflowAppWithLocalDatasets.WORDCOUNT_DATASET));
    Assert.assertNull(localDataset.get());

    fileSetDataset = getDataset(testSpace.dataset(WorkflowAppWithLocalDatasets.CSV_FILESET_DATASET));
    Assert.assertNull(fileSetDataset.get());

    // Verify that the workflow hasn't completed on its own before we signal it to
    history = wfManager.getHistory(ProgramRunStatus.RUNNING);
    Assert.assertEquals(1, history.size());

    // Signal the Workflow to continue
    doneFile.createNewFile();

    // Wait for workflow to finish
    wfManager.waitForRuns(ProgramRunStatus.COMPLETED, expectedComplete, 1, TimeUnit.MINUTES);
    Map<String, WorkflowNodeStateDetail> nodeStateDetailMap = wfManager.getWorkflowNodeStates(runId);
    Map<String, String> workflowMetricsContext = new HashMap<>();
    workflowMetricsContext.put(Constants.Metrics.Tag.NAMESPACE, testSpace.getNamespace());
    workflowMetricsContext.put(Constants.Metrics.Tag.APP, applicationManager.getInfo().getName());
    workflowMetricsContext.put(Constants.Metrics.Tag.WORKFLOW, WorkflowAppWithLocalDatasets.WORKFLOW_NAME);
    workflowMetricsContext.put(Constants.Metrics.Tag.RUN_ID, runId);

    Map<String, String> writerContext = new HashMap<>(workflowMetricsContext);
    writerContext.put(Constants.Metrics.Tag.NODE,
                      WorkflowAppWithLocalDatasets.LocalDatasetWriter.class.getSimpleName());

    Assert.assertEquals(2, getMetricsManager().getTotalMetric(writerContext, "user.num.lines"));

    Map<String, String> wfSparkMetricsContext = new HashMap<>(workflowMetricsContext);
    wfSparkMetricsContext.put(Constants.Metrics.Tag.NODE, "JavaSparkCSVToSpaceConverter");
    Assert.assertEquals(2, getMetricsManager().getTotalMetric(wfSparkMetricsContext, "user.num.lines"));

    // check in spark context
    Map<String, String> sparkMetricsContext = new HashMap<>();
    sparkMetricsContext.put(Constants.Metrics.Tag.NAMESPACE, testSpace.getNamespace());
    sparkMetricsContext.put(Constants.Metrics.Tag.APP, applicationManager.getInfo().getName());
    sparkMetricsContext.put(Constants.Metrics.Tag.SPARK, "JavaSparkCSVToSpaceConverter");
    sparkMetricsContext.put(Constants.Metrics.Tag.RUN_ID,
                            nodeStateDetailMap.get("JavaSparkCSVToSpaceConverter").getRunId());
    Assert.assertEquals(2, getMetricsManager().getTotalMetric(sparkMetricsContext, "user.num.lines"));

    Map<String, String> appMetricsContext = new HashMap<>();
    appMetricsContext.put(Constants.Metrics.Tag.NAMESPACE, testSpace.getNamespace());
    appMetricsContext.put(Constants.Metrics.Tag.APP, applicationManager.getInfo().getName());
    // app metrics context should have sum from custom action and spark metrics.
    Assert.assertEquals(4, getMetricsManager().getTotalMetric(appMetricsContext, "user.num.lines"));

    Map<String, String> wfMRMetricsContext = new HashMap<>(workflowMetricsContext);
    wfMRMetricsContext.put(Constants.Metrics.Tag.NODE, "WordCount");
    Assert.assertEquals(7, getMetricsManager().getTotalMetric(wfMRMetricsContext, "user.num.words"));

    // mr metrics context
    Map<String, String> mrMetricsContext = new HashMap<>();
    mrMetricsContext.put(Constants.Metrics.Tag.NAMESPACE, testSpace.getNamespace());
    mrMetricsContext.put(Constants.Metrics.Tag.APP, applicationManager.getInfo().getName());
    mrMetricsContext.put(Constants.Metrics.Tag.MAPREDUCE, "WordCount");
    mrMetricsContext.put(Constants.Metrics.Tag.RUN_ID,
                            nodeStateDetailMap.get("WordCount").getRunId());
    Assert.assertEquals(7, getMetricsManager().getTotalMetric(mrMetricsContext, "user.num.words"));

    final Map<String, String> readerContext = new HashMap<>(workflowMetricsContext);
    readerContext.put(Constants.Metrics.Tag.NODE, "readerAction");

    Tasks.waitFor(6L, new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return getMetricsManager().getTotalMetric(readerContext, "user.unique.words");
      }
    }, 60, TimeUnit.SECONDS);
    return runId;
  }

  @Category(XSlowTests.class)
  @Test
  @Ignore
  public void testDeployWorkflowApp() throws Exception {
    // Add test back when CDAP-12350 is resolved
    ApplicationManager applicationManager = deployApplication(testSpace, AppWithSchedule.class);
    final WorkflowManager wfmanager = applicationManager.getWorkflowManager(AppWithSchedule.WORKFLOW_NAME);
    List<ScheduleDetail> schedules = wfmanager.getProgramSchedules();
    Assert.assertEquals(2, schedules.size());
    String scheduleName = schedules.get(1).getName();
    Assert.assertNotNull(scheduleName);
    Assert.assertFalse(scheduleName.isEmpty());

    final int initialRuns = wfmanager.getHistory().size();
    LOG.info("initialRuns = {}", initialRuns);
    wfmanager.getSchedule(scheduleName).resume();

    String status = wfmanager.getSchedule(scheduleName).status(200);
    Assert.assertEquals("SCHEDULED", status);

    // Make sure something ran before suspending
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return wfmanager.getHistory().size() > 0;
      }
    }, 15, TimeUnit.SECONDS);

    wfmanager.getSchedule(scheduleName).suspend();
    waitForScheduleState(scheduleName, wfmanager, ProgramScheduleStatus.SUSPENDED);

    // All runs should be completed
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        for (RunRecord record : wfmanager.getHistory()) {
          if (record.getStatus() != ProgramRunStatus.COMPLETED) {
            return false;
          }
        }
        return true;
      }
    }, 15, TimeUnit.SECONDS);

    List<RunRecord> history = wfmanager.getHistory();
    int workflowRuns = history.size();
    LOG.info("workflowRuns = {}", workflowRuns);
    Assert.assertTrue(workflowRuns > 0);

    //Sleep for some time and verify there are no more scheduled jobs after the suspend.
    TimeUnit.SECONDS.sleep(5);
    final int workflowRunsAfterSuspend = wfmanager.getHistory().size();
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    wfmanager.getSchedule(scheduleName).resume();

    //Check that after resume it goes to "SCHEDULED" state
    waitForScheduleState(scheduleName, wfmanager, ProgramScheduleStatus.SCHEDULED);

    // Make sure new runs happens after resume
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return wfmanager.getHistory().size() > workflowRunsAfterSuspend;
      }
    }, 15, TimeUnit.SECONDS);

    // Check scheduled state
    Assert.assertEquals("SCHEDULED", wfmanager.getSchedule(scheduleName).status(200));

    // Check status of non-existent schedule
    Assert.assertEquals("NOT_FOUND", wfmanager.getSchedule("doesnt exist").status(404));

    // Suspend the schedule
    wfmanager.getSchedule(scheduleName).suspend();

    // Check that after suspend it goes to "SUSPENDED" state
    waitForScheduleState(scheduleName, wfmanager, ProgramScheduleStatus.SUSPENDED);

    // Test workflow token while suspended
    String pid = history.get(0).getPid();
    WorkflowTokenDetail workflowToken = wfmanager.getToken(pid, WorkflowToken.Scope.SYSTEM, null);
    Assert.assertEquals(0, workflowToken.getTokenData().size());
    workflowToken = wfmanager.getToken(pid, null, null);
    Assert.assertEquals(2, workflowToken.getTokenData().size());

    // Wait for all workflow runs to finish execution, in case more than one run happened with an enabled schedule
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        for (RunRecord record : wfmanager.getHistory()) {
          if (record.getStatus() != ProgramRunStatus.COMPLETED) {
            return false;
          }
        }
        return true;
      }
    }, 15, TimeUnit.SECONDS);

    // Verify workflow token after workflow completion
    WorkflowTokenNodeDetail workflowTokenAtNode =
      wfmanager.getTokenAtNode(pid, AppWithSchedule.DummyAction.class.getSimpleName(),
                               WorkflowToken.Scope.USER, "finished");
    Assert.assertEquals(true, Boolean.parseBoolean(workflowTokenAtNode.getTokenDataAtNode().get("finished")));
    workflowToken = wfmanager.getToken(pid, null, null);
    Assert.assertEquals(false, Boolean.parseBoolean(workflowToken.getTokenData().get("running").get(0).getValue()));
  }

  @Test
  public void testWorkflowCondition() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, ConditionalWorkflowApp.class);
    final WorkflowManager wfmanager = applicationManager.getWorkflowManager("ConditionalWorkflow");
    wfmanager.start(ImmutableMap.of("configurable.condition", "true"));
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return wfmanager.getHistory(ProgramRunStatus.COMPLETED).size() == 1;
      }
    }, 30, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    List<RunRecord> history = wfmanager.getHistory();
    String pid = history.get(0).getPid();
    WorkflowTokenNodeDetail tokenNodeDetail = wfmanager.getTokenAtNode(pid, "MyConfigurableCondition",
                                                                       WorkflowToken.Scope.USER, null);
    Map<String, String> expected = ImmutableMap.of("configurable.condition.initialize", "true",
                                                   "configurable.condition.destroy", "true",
                                                   "configurable.condition.apply", "true");
    Assert.assertEquals(expected, tokenNodeDetail.getTokenDataAtNode());

    tokenNodeDetail = wfmanager.getTokenAtNode(pid, "SimpleCondition", WorkflowToken.Scope.USER, null);
    expected = ImmutableMap.of("simple.condition.initialize", "true");
    Assert.assertEquals(expected, tokenNodeDetail.getTokenDataAtNode());

    tokenNodeDetail = wfmanager.getTokenAtNode(pid, "action2", WorkflowToken.Scope.USER, null);
    expected = ImmutableMap.of("action.name", "action2");
    Assert.assertEquals(expected, tokenNodeDetail.getTokenDataAtNode());
  }

  private void waitForScheduleState(final String scheduleId, final WorkflowManager wfmanager,
                                    ProgramScheduleStatus expected) throws Exception {
    Tasks.waitFor(expected, new Callable<ProgramScheduleStatus>() {
      @Override
      public ProgramScheduleStatus call() throws Exception {
        return ProgramScheduleStatus.valueOf(wfmanager.getSchedule(scheduleId).status(200));
      }
    }, 5, TimeUnit.SECONDS, 30, TimeUnit.MILLISECONDS);
  }

  @Category(SlowTests.class)
  @Test
  public void testGetServiceURL() throws Exception {
    ApplicationManager applicationManager = deployApplication(AppUsingGetServiceURL.class);
    ServiceManager centralServiceManager =
      applicationManager.getServiceManager(AppUsingGetServiceURL.CENTRAL_SERVICE).start();
    centralServiceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    WorkerManager pingingWorker = applicationManager.getWorkerManager(AppUsingGetServiceURL.PINGING_WORKER).start();

    // Test service's getServiceURL
    ServiceManager serviceManager = applicationManager.getServiceManager(AppUsingGetServiceURL.FORWARDING).start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    String result = callServiceGet(serviceManager.getServiceURL(), "ping");
    String decodedResult = new Gson().fromJson(result, String.class);
    // Verify that the service was able to hit the CentralService and retrieve the answer.
    Assert.assertEquals(AppUsingGetServiceURL.ANSWER, decodedResult);

    // Wait for the worker completed to make sure a value has been written to the dataset
    pingingWorker.waitForRun(ProgramRunStatus.COMPLETED, 30, TimeUnit.SECONDS);

    // Validate the value in the dataset by reading it via the service
    result = callServiceGet(serviceManager.getServiceURL(), "read/" + AppUsingGetServiceURL.DATASET_KEY);
    decodedResult = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppUsingGetServiceURL.ANSWER, decodedResult);

    serviceManager.stop();
    centralServiceManager.stop();

    serviceManager.waitForStopped(10, TimeUnit.SECONDS);
    centralServiceManager.waitForStopped(10, TimeUnit.SECONDS);
  }

  @Category(SlowTests.class)
  @Test
  public void testGetServiceURLDiffNamespace() throws Exception {
    ApplicationManager defaultApplicationManager = deployApplication(AppUsingGetServiceURL.class);
    ServiceManager defaultForwardingServiceManager = defaultApplicationManager
      .getServiceManager(AppUsingGetServiceURL.FORWARDING).start();
    defaultForwardingServiceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    ServiceManager defaultCentralServiceManager = defaultApplicationManager
      .getServiceManager(AppUsingGetServiceURL.CENTRAL_SERVICE).start();
    defaultCentralServiceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    // Call non-existent namespace from default
    String path = "forward/nonexitent";
    int responseCode = callServiceGetResponseCode(defaultForwardingServiceManager.getServiceURL(), path);
    Assert.assertEquals(404, responseCode);

    // Call non-initialized system namespace from default
    ApplicationManager systemApplicationManager = deployApplication(NamespaceId.SYSTEM, AppUsingGetServiceURL.class);

    path = "forward/" + NamespaceId.SYSTEM.getNamespace();
    responseCode = callServiceGetResponseCode(defaultForwardingServiceManager.getServiceURL(), path);
    Assert.assertEquals(404, responseCode);

    // Call system app from default
    ServiceManager systemForwardingServiceManager = systemApplicationManager
      .getServiceManager(AppUsingGetServiceURL.FORWARDING).start();
    systemForwardingServiceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    ServiceManager systemCentralServiceManager = systemApplicationManager
      .getServiceManager(AppUsingGetServiceURL.CENTRAL_SERVICE).start();
    systemCentralServiceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    String result = callServiceGet(defaultForwardingServiceManager.getServiceURL(), path);
    result = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppUsingGetServiceURL.ANSWER, result);

    // Call default app from system
    path = "forward/" + NamespaceId.DEFAULT.getNamespace();
    result = callServiceGet(systemForwardingServiceManager.getServiceURL(), path);
    result = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppUsingGetServiceURL.ANSWER, result);

    // Call stopped system app from default
    defaultCentralServiceManager.stop();
    defaultForwardingServiceManager.stop();
    defaultCentralServiceManager.waitForStopped(10, TimeUnit.SECONDS);
    defaultForwardingServiceManager.waitForStopped(10, TimeUnit.SECONDS);

    responseCode = callServiceGetResponseCode(systemForwardingServiceManager.getServiceURL(), path);
    Assert.assertEquals(404, responseCode);

    systemCentralServiceManager.stop();
    systemForwardingServiceManager.stop();
    systemCentralServiceManager.waitForStopped(10, TimeUnit.SECONDS);
    systemForwardingServiceManager.waitForStopped(10, TimeUnit.SECONDS);
  }

  /**
   * Checks to ensure that a particular  {@param workerManager} has {@param expected} number of
   * instances while retrying every 50 ms for 15 seconds.
   *
   * @throws Exception if the worker does not have the specified number of instances after 15 seconds.
   */
  private void workerInstancesCheck(final WorkerManager workerManager, int expected) throws Exception {
    Tasks.waitFor(expected, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return workerManager.getInstances();
      }
    }, 15, TimeUnit.SECONDS);
  }

  /**
   * Checks to ensure that a particular key is present in a {@link KeyValueTable}
   * @param namespace {@link NamespaceId}
   * @param datasetName name of the dataset
   * @param expectedKey expected key byte array
   *
   * @throws Exception if the key is not found even after 15 seconds of timeout
   */
  private void kvTableKeyCheck(final NamespaceId namespace, final String datasetName, final byte[] expectedKey)
    throws Exception {
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        DataSetManager<KeyValueTable> datasetManager = getDataset(namespace.dataset(datasetName));
        KeyValueTable kvTable = datasetManager.get();
        return kvTable.read(expectedKey) != null;
      }
    }, 15, TimeUnit.SECONDS);
  }

  @Category(SlowTests.class)
  @Test
  public void testWorkerInstances() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, AppUsingGetServiceURL.class);
    WorkerManager workerManager = applicationManager.getWorkerManager(AppUsingGetServiceURL.PINGING_WORKER).start();
    workerManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    // Should be 5 instances when first started.
    workerInstancesCheck(workerManager, 5);

    // Test increasing instances.
    workerManager.setInstances(10);
    workerInstancesCheck(workerManager, 10);

    // Test decreasing instances.
    workerManager.setInstances(2);
    workerInstancesCheck(workerManager, 2);

    // Test requesting same number of instances.
    workerManager.setInstances(2);
    workerInstancesCheck(workerManager, 2);

    WorkerManager lifecycleWorkerManager = applicationManager.getWorkerManager(AppUsingGetServiceURL.LIFECYCLE_WORKER);
    lifecycleWorkerManager.setInstances(3);
    lifecycleWorkerManager.start().waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    workerInstancesCheck(lifecycleWorkerManager, 3);
    for (int i = 0; i < 3; i++) {
      kvTableKeyCheck(testSpace, AppUsingGetServiceURL.WORKER_INSTANCES_DATASET,
                      Bytes.toBytes(String.format("init.%d", i)));
    }

    // Set 5 instances for the LifecycleWorker
    lifecycleWorkerManager.setInstances(5);
    workerInstancesCheck(lifecycleWorkerManager, 5);

    // Make sure all the keys have been written before stopping the worker
    for (int i = 0; i < 5; i++) {
      kvTableKeyCheck(testSpace, AppUsingGetServiceURL.WORKER_INSTANCES_DATASET,
                      Bytes.toBytes(String.format("init.%d", i)));
    }

    lifecycleWorkerManager.stop();
    lifecycleWorkerManager.waitForStopped(10, TimeUnit.SECONDS);

    if (workerManager.isRunning()) {
      workerManager.stop();
    }
    workerManager.waitForStopped(10, TimeUnit.SECONDS);

    // Should be same instances after being stopped.
    workerInstancesCheck(lifecycleWorkerManager, 5);
    workerInstancesCheck(workerManager, 2);

    // Assert the LifecycleWorker dataset writes
    // 3 workers should have started with 3 total instances. 2 more should later start with 5 total instances.
    assertWorkerDatasetWrites(Bytes.toBytes("init"),
                              Bytes.stopKeyForPrefix(Bytes.toBytes("init.2")), 3, 3);
    assertWorkerDatasetWrites(Bytes.toBytes("init.3"),
                              Bytes.stopKeyForPrefix(Bytes.toBytes("init")), 2, 5);

    // Test that the worker had 5 instances when stopped, and each knew that there were 5 instances
    byte[] startRow = Bytes.toBytes("stop");
    assertWorkerDatasetWrites(startRow, Bytes.stopKeyForPrefix(startRow), 5, 5);
  }

  private void assertWorkerDatasetWrites(byte[] startRow, byte[] endRow,
                                         int expectedCount, int expectedTotalCount) throws Exception {
    DataSetManager<KeyValueTable> datasetManager =
      getDataset(testSpace.dataset(AppUsingGetServiceURL.WORKER_INSTANCES_DATASET));
    KeyValueTable instancesTable = datasetManager.get();
    try (CloseableIterator<KeyValue<byte[], byte[]>> instancesIterator = instancesTable.scan(startRow, endRow)) {
      List<KeyValue<byte[], byte[]>> workerInstances = Lists.newArrayList(instancesIterator);
      // Assert that the worker starts with expectedCount instances
      Assert.assertEquals(expectedCount, workerInstances.size());
      // Assert that each instance of the worker knows the total number of instances
      for (KeyValue<byte[], byte[]> keyValue : workerInstances) {
        Assert.assertEquals(expectedTotalCount, Bytes.toInt(keyValue.getValue()));
      }
    }
  }

  // AppFabricClient returns IllegalStateException if the app fails to deploy
  @Category(SlowTests.class)
  @Test(expected = IllegalStateException.class)
  public void testServiceWithInvalidHandler() throws Exception {
    deployApplication(AppWithInvalidHandler.class);
  }

  @Category(SlowTests.class)
  @Test
  public void testAppWithWorker() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, AppWithWorker.class);
    LOG.info("Deployed.");
    WorkerManager manager = applicationManager.getWorkerManager(AppWithWorker.WORKER).start();

    // Wait for initialize and run states
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        DataSetManager<KeyValueTable> dataSetManager = getDataset(testSpace.dataset(AppWithWorker.DATASET));
        KeyValueTable table = dataSetManager.get();
        return AppWithWorker.INITIALIZE.equals(Bytes.toString(table.read(AppWithWorker.INITIALIZE))) &&
          AppWithWorker.RUN.equals(Bytes.toString(table.read(AppWithWorker.RUN)));
      }
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    manager.stop();
    applicationManager.stopAll();

    // Wait for stop state
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        DataSetManager<KeyValueTable> dataSetManager = getDataset(testSpace.dataset(AppWithWorker.DATASET));
        KeyValueTable table = dataSetManager.get();
        return AppWithWorker.STOP.equals(Bytes.toString(table.read(AppWithWorker.STOP)));
      }
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  //CDAP-18061 opened for tacking fix of this flaky test case
  @Ignore
  @Category(SlowTests.class)
  @Test
  public void testAppWithTxTimeout() throws Exception {
    int txDefaulTimeoutService = 17;
    int txDefaulTimeoutWorker = 18;
    int txDefaulTimeoutWorkflow = 19;
    int txDefaulTimeoutAction = 20;
    int txDefaulTimeoutMapReduce = 23;
    int txDefaulTimeoutSpark = 24;

    final ApplicationManager appManager = deployApplication(testSpace, AppWithCustomTx.class);
    try {
      // attempt to start with a tx timeout that exceeds the max tx timeout
      appManager.getServiceManager(AppWithCustomTx.SERVICE).start(txTimeoutArguments(100000));

      // wait for the failed status of AppWithCustomTx.SERVICE to be persisted, so that it can be started again
      Tasks.waitFor(1,
                    () -> appManager.getServiceManager(AppWithCustomTx.SERVICE)
                      .getHistory(ProgramRunStatus.FAILED).size(),
                    30L, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
      String tmpDir = TMP_FOLDER.newFolder().getAbsolutePath();
      Map<String, String> serviceArgs = new HashMap<>(txTimeoutArguments(txDefaulTimeoutService));
      serviceArgs.put("done.dir", tmpDir);
      ServiceManager serviceManager = appManager.getServiceManager(AppWithCustomTx.SERVICE).start(serviceArgs);
      WorkerManager notxWorkerManager = appManager.getWorkerManager(AppWithCustomTx.WORKER_NOTX)
        .start(txTimeoutArguments(txDefaulTimeoutWorker));
      WorkerManager txWorkerManager = appManager.getWorkerManager(AppWithCustomTx.WORKER_TX)
        .start(txTimeoutArguments(txDefaulTimeoutWorker));
      WorkflowManager txWFManager = appManager.getWorkflowManager(AppWithCustomTx.WORKFLOW_TX)
        .start(txTimeoutArguments(txDefaulTimeoutWorkflow));
      WorkflowManager notxWFManager = appManager.getWorkflowManager(AppWithCustomTx.WORKFLOW_NOTX)
        .start(txTimeoutArguments(txDefaulTimeoutWorkflow,
                                  txDefaulTimeoutAction, "action", AppWithCustomTx.ACTION_NOTX));
      MapReduceManager txMRManager = appManager.getMapReduceManager(AppWithCustomTx.MAPREDUCE_TX)
        .start(txTimeoutArguments(txDefaulTimeoutMapReduce));
      MapReduceManager notxMRManager = appManager.getMapReduceManager(AppWithCustomTx.MAPREDUCE_NOTX)
        .start(txTimeoutArguments(txDefaulTimeoutMapReduce));
      SparkManager txSparkManager = appManager.getSparkManager(AppWithCustomTx.SPARK_TX)
        .start(txTimeoutArguments(txDefaulTimeoutSpark));
      SparkManager notxSparkManager = appManager.getSparkManager(AppWithCustomTx.SPARK_NOTX)
        .start(txTimeoutArguments(txDefaulTimeoutSpark));

      serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

      callServicePut(serviceManager.getServiceURL(), "tx", "hello", 200, new File(tmpDir, "txhello").getAbsolutePath());
      callServicePut(serviceManager.getServiceURL(), "tx", AppWithCustomTx.FAIL_PRODUCER, 200);
      callServicePut(serviceManager.getServiceURL(), "tx", AppWithCustomTx.FAIL_CONSUMER, 500);
      callServicePut(serviceManager.getServiceURL(), "notx", "hello", 200,
                     new File(tmpDir, "notxhello").getAbsolutePath());
      callServicePut(serviceManager.getServiceURL(), "notx", AppWithCustomTx.FAIL_PRODUCER, 200);
      callServicePut(serviceManager.getServiceURL(), "notx", AppWithCustomTx.FAIL_CONSUMER, 500);


      serviceManager.stop();
      serviceManager.waitForStopped(10, TimeUnit.SECONDS);

      txMRManager.waitForRun(ProgramRunStatus.FAILED, 10L, TimeUnit.SECONDS);
      notxMRManager.waitForRun(ProgramRunStatus.FAILED, 10L, TimeUnit.SECONDS);
      txSparkManager.waitForRun(ProgramRunStatus.COMPLETED, 10L, TimeUnit.SECONDS);
      notxSparkManager.waitForRun(ProgramRunStatus.COMPLETED, 10L, TimeUnit.SECONDS);
      notxWorkerManager.waitForRun(ProgramRunStatus.COMPLETED, 10L, TimeUnit.SECONDS);
      txWorkerManager.waitForRun(ProgramRunStatus.COMPLETED, 10L, TimeUnit.SECONDS);
      txWFManager.waitForRun(ProgramRunStatus.COMPLETED, 10L, TimeUnit.SECONDS);
      notxWFManager.waitForRun(ProgramRunStatus.COMPLETED, 10L, TimeUnit.SECONDS);

      DataSetManager<TransactionCapturingTable> dataset = getDataset(testSpace.dataset(AppWithCustomTx.CAPTURE));
      Table t = dataset.get().getTable();

      // all programs attempt to write to the table in different transactional contexts
      // - if it is outside a transaction, then the expected value is null
      // - if it is in an attempt of a nested transaction, then the expected value is null
      // - if it is in an implicit transaction, then the expected value is "default"
      // - if it is in an explicit transaction, then the expected value is the transaction timeout
      Object[][] writesToValidate = new Object[][] {

        // transactions attempted by the workers
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.INITIALIZE, txDefaulTimeoutWorker },
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutWorker },
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutWorker},
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_WORKER_RUNTIME },
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.INITIALIZE, null },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.INITIALIZE_TX, AppWithCustomTx.TIMEOUT_WORKER_INITIALIZE },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.INITIALIZE_TX_D, txDefaulTimeoutWorker },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_WORKER_DESTROY },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutWorker },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_WORKER_RUNTIME },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutWorker },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },

        // transactions attempted by the service
        { AppWithCustomTx.HANDLER_TX, AppWithCustomTx.INITIALIZE, txDefaulTimeoutService },
        { AppWithCustomTx.HANDLER_TX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.HANDLER_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutService },
        { AppWithCustomTx.HANDLER_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.HANDLER_TX, AppWithCustomTx.RUNTIME, txDefaulTimeoutService },
        { AppWithCustomTx.HANDLER_TX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_CONSUMER_RUNTIME },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME_TX_T, AppWithCustomTx.TIMEOUT_CONSUMER_RUNTIME },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME_NEST_T, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME_NEST_CT, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME_NEST_TC, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutService },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.ONERROR, txDefaulTimeoutService },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.ONERROR_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_PRODUCER_RUNTIME },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME_TX_T, AppWithCustomTx.TIMEOUT_PRODUCER_RUNTIME },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME_NEST_T, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME_NEST_CT, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME_NEST_TC, AppWithCustomTx.FAILED },
        //TODO: CDAP-17920 - Fix assertion flackiness, commented out for now
        //{ AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutService },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.ONERROR, txDefaulTimeoutService },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.ONERROR_NEST, AppWithCustomTx.FAILED },

        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.INITIALIZE, null },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.INITIALIZE_TX, AppWithCustomTx.TIMEOUT_HANDLER_INITIALIZE },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.INITIALIZE_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_HANDLER_DESTROY },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_HANDLER_RUNTIME },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_CONSUMER_RUNTIME },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME_TX_T, AppWithCustomTx.TIMEOUT_CONSUMER_RUNTIME },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME_NEST_T, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME_NEST_CT, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME_NEST_TC, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_CONSUMER_DESTROY },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutService},
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.ONERROR, null },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.ONERROR_TX, AppWithCustomTx.TIMEOUT_CONSUMER_ERROR },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.ONERROR_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.ONERROR_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_PRODUCER_RUNTIME },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME_TX_T, AppWithCustomTx.TIMEOUT_PRODUCER_RUNTIME },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME_NEST_T, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME_NEST_CT, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME_NEST_TC, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_PRODUCER_DESTROY },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.ONERROR, null },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.ONERROR_TX, AppWithCustomTx.TIMEOUT_PRODUCER_ERROR },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.ONERROR_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.ONERROR_NEST, AppWithCustomTx.FAILED },

        // transactions attempted by the workflows
        { AppWithCustomTx.WORKFLOW_TX, AppWithCustomTx.INITIALIZE, txDefaulTimeoutWorkflow },
        { AppWithCustomTx.WORKFLOW_TX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.WORKFLOW_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutWorkflow },
        { AppWithCustomTx.WORKFLOW_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.INITIALIZE, txDefaulTimeoutWorkflow },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_ACTION_RUNTIME },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutWorkflow },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutWorkflow },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },

        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.INITIALIZE, null },
        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.INITIALIZE_TX, AppWithCustomTx.TIMEOUT_WORKFLOW_INITIALIZE },
        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.INITIALIZE_TX_D, txDefaulTimeoutWorkflow },
        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_WORKFLOW_DESTROY },
        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutWorkflow },
        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.INITIALIZE, null },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.INITIALIZE_TX, AppWithCustomTx.TIMEOUT_ACTION_INITIALIZE },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.INITIALIZE_TX_D, txDefaulTimeoutAction },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_ACTION_RUNTIME },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutAction },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_ACTION_DESTROY },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutAction },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },

        // transactions attempted by the mapreduce's
        { AppWithCustomTx.MAPREDUCE_TX, AppWithCustomTx.INITIALIZE, txDefaulTimeoutMapReduce },
        { AppWithCustomTx.MAPREDUCE_TX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.MAPREDUCE_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutMapReduce },
        { AppWithCustomTx.MAPREDUCE_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.INITIALIZE, null },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.INITIALIZE_TX, AppWithCustomTx.TIMEOUT_MAPREDUCE_INITIALIZE },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.INITIALIZE_TX_D, txDefaulTimeoutMapReduce },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_MAPREDUCE_DESTROY },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutMapReduce },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },

        // transactions attempted by the spark's
        { AppWithCustomTx.SPARK_TX, AppWithCustomTx.INITIALIZE, txDefaulTimeoutSpark },
        { AppWithCustomTx.SPARK_TX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.SPARK_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutSpark },
        { AppWithCustomTx.SPARK_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.INITIALIZE, null },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.INITIALIZE_TX, AppWithCustomTx.TIMEOUT_SPARK_INITIALIZE },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.INITIALIZE_TX_D, txDefaulTimeoutSpark },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_SPARK_DESTROY },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutSpark },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
      };

      for (Object[] writeToValidate : writesToValidate) {
        String row = (String) writeToValidate[0];
        String column = (String) writeToValidate[1];
        String expectedValue = writeToValidate[2] == null ? null : String.valueOf(writeToValidate[2]);
        Assert.assertEquals("Error for " + row + "." + column,
                            expectedValue, t.get(new Get(row, column)).getString(column));
      }

    } finally {
      appManager.stopAll();
    }
  }

  private static Map<String, String> txTimeoutArguments(int timeout) {
    return ImmutableMap.of(SystemArguments.TRANSACTION_TIMEOUT, String.valueOf(timeout));
  }

  private static Map<String, String> txTimeoutArguments(int timeout, int timeoutForScope, String scope, String name) {
    return ImmutableMap.of(SystemArguments.TRANSACTION_TIMEOUT, String.valueOf(timeout),
                           String.format("%s.%s.%s", scope, name, SystemArguments.TRANSACTION_TIMEOUT),
                           String.valueOf(timeoutForScope));
  }

  @Test
  public void testWorkerStop() throws Exception {
    // Test to make sure the worker program's status goes to stopped after the run method finishes
    ApplicationManager manager = deployApplication(NoOpWorkerApp.class);
    WorkerManager workerManager = manager.getWorkerManager("NoOpWorker");
    int numRuns = workerManager.getHistory().size();
    workerManager.start();
    Tasks.waitFor(numRuns + 1, () -> workerManager.getHistory().size(), 30, TimeUnit.SECONDS);
    workerManager.waitForStopped(30, TimeUnit.SECONDS);
  }

  @Category(SlowTests.class)
  @Test
  public void testAppWithServices() throws Exception {
    ApplicationManager applicationManager = deployApplication(AppWithServices.class);
    LOG.info("Deployed.");
    ServiceManager serviceManager = applicationManager.getServiceManager(AppWithServices.SERVICE_NAME).start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    LOG.info("Service Started");

    URL serviceURL = serviceManager.getServiceURL(15, TimeUnit.SECONDS);
    Assert.assertNotNull(serviceURL);

    // Call the ping endpoint
    URL url = new URL(serviceURL, "ping2");
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = executeHttp(request);
    Assert.assertEquals(200, response.getResponseCode());

    // Call the failure endpoint
    url = new URL(serviceURL, "failure");
    request = HttpRequest.get(url).build();
    response = executeHttp(request);
    Assert.assertEquals(500, response.getResponseCode());
    Assert.assertTrue(response.getResponseBodyAsString().contains("Exception"));

    // Call the verify ClassLoader endpoint
    url = new URL(serviceURL, "verifyClassLoader");
    request = HttpRequest.get(url).build();
    response = executeHttp(request);
    Assert.assertEquals(200, response.getResponseCode());

    RuntimeMetrics serviceMetrics = serviceManager.getMetrics();
    serviceMetrics.waitForinput(3, 5, TimeUnit.SECONDS);
    Assert.assertEquals(3, serviceMetrics.getInput());
    Assert.assertEquals(2, serviceMetrics.getProcessed());
    Assert.assertEquals(1, serviceMetrics.getException());

    // in the AppWithServices the handlerName is same as the serviceName - "ServerService" handler
    RuntimeMetrics handlerMetrics = getMetricsManager().getServiceHandlerMetrics(NamespaceId.DEFAULT.getNamespace(),
                                                                                 AppWithServices.APP_NAME,
                                                                                 AppWithServices.SERVICE_NAME,
                                                                                 AppWithServices.SERVICE_NAME);
    handlerMetrics.waitForinput(3, 5, TimeUnit.SECONDS);
    Assert.assertEquals(3, handlerMetrics.getInput());
    Assert.assertEquals(2, handlerMetrics.getProcessed());
    Assert.assertEquals(1, handlerMetrics.getException());

    // we can verify metrics, by adding getServiceMetrics in MetricsManager and then disabling the system scope test in
    // TestMetricsCollectionService

    LOG.info("DatasetUpdateService Started");
    Map<String, String> args
      = ImmutableMap.of(AppWithServices.WRITE_VALUE_RUN_KEY, AppWithServices.DATASET_TEST_VALUE,
                        AppWithServices.WRITE_VALUE_STOP_KEY, AppWithServices.DATASET_TEST_VALUE_STOP);
    ServiceManager datasetWorkerServiceManager = applicationManager
      .getServiceManager(AppWithServices.DATASET_WORKER_SERVICE_NAME).start(args);
    WorkerManager datasetWorker =
      applicationManager.getWorkerManager(AppWithServices.DATASET_UPDATE_WORKER).start(args);

    datasetWorker.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    datasetWorkerServiceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    ServiceManager noopManager = applicationManager.getServiceManager("NoOpService").start();
    noopManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    // We don't know when the datasetWorker run() method executed, hence need to retry on the service call
    // until it can get a value from a dataset, which was written out by the datasetWorker.
    AtomicInteger called = new AtomicInteger(0);
    AtomicInteger failed = new AtomicInteger(0);
    Tasks.waitFor(AppWithServices.DATASET_TEST_VALUE, new Callable<String>() {
      @Override
      public String call() throws Exception {
        URL url = noopManager.getServiceURL();
        String path = "ping/" + AppWithServices.DATASET_TEST_KEY;
        try {
          called.incrementAndGet();
          return new Gson().fromJson(callServiceGet(url, path), String.class);
        } catch (IOException e) {
          failed.incrementAndGet();
          LOG.debug("Exception when reading from service {}/{}", url, path, e);
        }
        return null;
      }
    }, 30, TimeUnit.SECONDS);

    // Validates the metrics emitted by the service call.
    handlerMetrics = getMetricsManager().getServiceHandlerMetrics(NamespaceId.DEFAULT.getNamespace(),
                                                                  AppWithServices.APP_NAME,
                                                                  "NoOpService",
                                                                  "NoOpHandler");
    handlerMetrics.waitForinput(called.get(), 5, TimeUnit.SECONDS);
    handlerMetrics.waitForProcessed(1, 5, TimeUnit.SECONDS);
    handlerMetrics.waitForException(failed.get(), 5, TimeUnit.SECONDS);

    // Test that a service can discover another service
    String path = String.format("discover/%s/%s",
                                AppWithServices.APP_NAME, AppWithServices.DATASET_WORKER_SERVICE_NAME);
    url = new URL(serviceURL, path);
    request = HttpRequest.get(url).build();
    response = executeHttp(request);
    Assert.assertEquals(200, response.getResponseCode());

    datasetWorker.stop();
    datasetWorker.waitForStopped(10, TimeUnit.SECONDS);

    datasetWorkerServiceManager.stop();
    datasetWorkerServiceManager.waitForStopped(10, TimeUnit.SECONDS);
    LOG.info("DatasetUpdateService Stopped");
    serviceManager.stop();
    serviceManager.waitForStopped(10, TimeUnit.SECONDS);
    LOG.info("ServerService Stopped");

    // Since all worker are stopped, we can just hit the service to read the dataset. No retry needed.
    String result = callServiceGet(noopManager.getServiceURL(), "ping/" + AppWithServices.DATASET_TEST_KEY_STOP);
    String decodedResult = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppWithServices.DATASET_TEST_VALUE_STOP, decodedResult);

    result = callServiceGet(noopManager.getServiceURL(), "ping/" + AppWithServices.DATASET_TEST_KEY_STOP_2);
    decodedResult = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppWithServices.DATASET_TEST_VALUE_STOP_2, decodedResult);
  }

  @Test
  public void testTransactionHandlerService() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, AppWithServices.class);
    LOG.info("Deployed.");

    ServiceManager serviceManager =
      applicationManager.getServiceManager(AppWithServices.TRANSACTIONS_SERVICE_NAME).start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    LOG.info("Service Started");

    final URL baseUrl = serviceManager.getServiceURL(15, TimeUnit.SECONDS);
    Assert.assertNotNull(baseUrl);

    // Make a request to write in a separate thread and wait for it to return.
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future<Integer> requestFuture = executorService.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        try {
          URL url = new URL(String.format("%s/write/%s/%s/%d",
                                          baseUrl,
                                          AppWithServices.DATASET_TEST_KEY,
                                          AppWithServices.DATASET_TEST_VALUE,
                                          10000));
          HttpRequest request = HttpRequest.get(url).build();
          HttpResponse response = executeHttp(request);
          return response.getResponseCode();
        } catch (Exception e) {
          LOG.error("Request thread got exception.", e);
          throw Throwables.propagate(e);
        }
      }
    });

    // The dataset should not be written by the time this request is made, since the transaction to write
    // has not been committed yet.
    URL url = new URL(String.format("%s/read/%s", baseUrl, AppWithServices.DATASET_TEST_KEY));
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = executeHttp(request);
    Assert.assertEquals(204, response.getResponseCode());

    // Wait for the transaction to commit.
    Integer writeStatusCode = requestFuture.get();
    Assert.assertEquals(200, writeStatusCode.intValue());

    // Make the same request again. By now the transaction should've completed.
    request = HttpRequest.get(url).build();
    response = executeHttp(request);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(AppWithServices.DATASET_TEST_VALUE,
                        new Gson().fromJson(response.getResponseBodyAsString(), String.class));

    executorService.shutdown();
    serviceManager.stop();
    serviceManager.waitForStopped(10, TimeUnit.SECONDS);

    DataSetManager<KeyValueTable> dsManager = getDataset(testSpace.dataset(AppWithServices.TRANSACTIONS_DATASET_NAME));
    String value = Bytes.toString(dsManager.get().read(AppWithServices.DESTROY_KEY));
    Assert.assertEquals(AppWithServices.VALUE, value);
  }

  @Test
  public void testAppRedeployKeepsData() throws Exception {
    deployApplication(testSpace, AppWithTable.class);
    DataSetManager<Table> myTableManager = getDataset(testSpace.dataset("my_table"));
    myTableManager.get().put(new Put("key1", "column1", "value1"));
    myTableManager.flush();

    // Changes should be visible to other instances of datasets
    DataSetManager<Table> myTableManager2 = getDataset(testSpace.dataset("my_table"));
    Assert.assertEquals("value1", myTableManager2.get().get(new Get("key1", "column1")).getString("column1"));

    // Even after redeploy of an app: changes should be visible to other instances of datasets
    deployApplication(AppWithTable.class);
    DataSetManager<Table> myTableManager3 = getDataset(testSpace.dataset("my_table"));
    Assert.assertEquals("value1", myTableManager3.get().get(new Get("key1", "column1")).getString("column1"));

    // Calling commit again (to test we can call it multiple times)
    myTableManager.get().put(new Put("key1", "column1", "value2"));
    myTableManager.flush();

    Assert.assertEquals("value1", myTableManager3.get().get(new Get("key1", "column1")).getString("column1"));
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoDeployDatasetModule() throws Exception {
    testAppWithDataset(AppsWithDataset.AppWithAutoDeploy.class, "MyService");
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoDeployDataset() throws Exception {
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    // we should be fine if module is already there. Deploy of module should not happen
    testAppWithDataset(AppsWithDataset.AppWithAutoDeploy.class, "MyService");
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoCreateDataset() throws Exception {
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    testAppWithDataset(AppsWithDataset.AppWithAutoCreate.class, "MyService");
  }

  @Test(timeout = 60000L)
  public void testAppWithExistingDataset() throws Exception {
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    addDatasetInstance("myKeyValueTable", "myTable", DatasetProperties.EMPTY);
    testAppWithDataset(AppsWithDataset.AppWithExisting.class, "MyService");
  }

  @Test(timeout = 60000L)
  public void testAppWithExistingDatasetInjectedByAnnotation() throws Exception {
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    addDatasetInstance("myKeyValueTable", "myTable", DatasetProperties.EMPTY);
    testAppWithDataset(AppsWithDataset.AppUsesAnnotation.class, "MyServiceWithUseDataSetAnnotation");
  }

  @Test(timeout = 60000L)
  public void testDatasetWithoutApp() throws Exception {
    // TODO: Although this has nothing to do with this testcase, deploying a dummy app to create the default namespace
    deployApplication(testSpace, DummyApp.class);
    deployDatasetModule(testSpace.datasetModule("my-kv"), AppsWithDataset.KeyValueTableDefinition.Module.class);
    addDatasetInstance("myKeyValueTable", testSpace.dataset("myTable"), DatasetProperties.EMPTY);
    DataSetManager<AppsWithDataset.KeyValueTableDefinition.KeyValueTable> dataSetManager =
      getDataset(testSpace.dataset("myTable"));
    AppsWithDataset.KeyValueTableDefinition.KeyValueTable kvTable = dataSetManager.get();
    kvTable.put("test", "hello");
    dataSetManager.flush();
    Assert.assertEquals("hello", dataSetManager.get().get("test"));
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoDeployDatasetType() throws Exception {
    testAppWithDataset(AppsWithDataset.AppWithAutoDeployType.class, "MyService");
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoDeployDatasetTypeShortcut() throws Exception {
    testAppWithDataset(AppsWithDataset.AppWithAutoDeployTypeShortcut.class, "MyService");
  }

  @Test
  public void testClusterName() throws Exception {
    String clusterName = getConfiguration().get(Constants.CLUSTER_NAME);

    ApplicationManager appManager = deployApplication(ClusterNameTestApp.class);
    final DataSetManager<KeyValueTable> datasetManager = getDataset(ClusterNameTestApp.CLUSTER_NAME_TABLE);
    final KeyValueTable clusterNameTable = datasetManager.get();

    // A callable for reading the cluster name from the ClusterNameTable.
    // It is used for Tasks.waitFor call down below.
    final AtomicReference<String> key = new AtomicReference<>();
    Callable<String> readClusterName = new Callable<String>() {
      @Nullable
      @Override
      public String call() throws Exception {
        datasetManager.flush();
        byte[] bytes = clusterNameTable.read(key.get());
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
      }
    };

    // Service
    ServiceManager serviceManager = appManager.getServiceManager(
      ClusterNameTestApp.ClusterNameServiceHandler.class.getSimpleName()).start();
    Assert.assertEquals(clusterName, callServiceGet(serviceManager.getServiceURL(10, TimeUnit.SECONDS), "clusterName"));
    serviceManager.stop();

    // Worker
    WorkerManager workerManager = appManager.getWorkerManager(
      ClusterNameTestApp.ClusterNameWorker.class.getSimpleName()).start();
    key.set("worker.cluster.name");
    Tasks.waitFor(clusterName, readClusterName, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    // The worker will stop by itself. No need to call stop
    workerManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.SECONDS);

    // MapReduce
    // Setup the input file used by MR
    Location location = this.<FileSet>getDataset(ClusterNameTestApp.INPUT_FILE_SET).get().getLocation("input");
    try (PrintStream printer = new PrintStream(location.getOutputStream(), true, "UTF-8")) {
      for (int i = 0; i < 10; i++) {
        printer.println("Hello World " + i);
      }
    }

    // Setup input and output dataset arguments
    Map<String, String> inputArgs = new HashMap<>();
    FileSetArguments.setInputPath(inputArgs, "input");
    Map<String, String> outputArgs = new HashMap<>();
    FileSetArguments.setOutputPath(outputArgs, "output");

    Map<String, String> args = new HashMap<>();
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, ClusterNameTestApp.INPUT_FILE_SET, inputArgs));
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, ClusterNameTestApp.OUTPUT_FILE_SET, outputArgs));

    MapReduceManager mrManager = appManager.getMapReduceManager(
      ClusterNameTestApp.ClusterNameMapReduce.class.getSimpleName()).start(args);
    key.set("mr.client.cluster.name");
    Tasks.waitFor(clusterName, readClusterName, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    key.set("mapper.cluster.name");
    Tasks.waitFor(clusterName, readClusterName, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    key.set("reducer.cluster.name");
    Tasks.waitFor(clusterName, readClusterName, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    mrManager.waitForRun(ProgramRunStatus.COMPLETED, 60, TimeUnit.SECONDS);

    // Spark
    SparkManager sparkManager = appManager.getSparkManager(
      ClusterNameTestApp.ClusterNameSpark.class.getSimpleName()).start();
    key.set("spark.cluster.name");
    Tasks.waitFor(clusterName, readClusterName, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 60, TimeUnit.SECONDS);

    // Workflow
    // Cleanup the output path for the MR job in the workflow first
    this.<FileSet>getDataset(ClusterNameTestApp.OUTPUT_FILE_SET).get().getLocation("output").delete(true);
    args = RuntimeArguments.addScope(Scope.MAPREDUCE,
                                     ClusterNameTestApp.ClusterNameMapReduce.class.getSimpleName(), args);
    WorkflowManager workflowManager = appManager.getWorkflowManager(
      ClusterNameTestApp.ClusterNameWorkflow.class.getSimpleName()).start(args);

    String prefix = ClusterNameTestApp.ClusterNameWorkflow.class.getSimpleName() + ".";
    key.set(prefix + "mr.client.cluster.name");
    Tasks.waitFor(clusterName, readClusterName, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    key.set(prefix + "mapper.cluster.name");
    Tasks.waitFor(clusterName, readClusterName, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    key.set(prefix + "reducer.cluster.name");
    Tasks.waitFor(clusterName, readClusterName, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    key.set(prefix + "spark.cluster.name");
    Tasks.waitFor(clusterName, readClusterName, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    key.set(prefix + "action.cluster.name");
    Tasks.waitFor(clusterName, readClusterName, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 120, TimeUnit.SECONDS);
  }

  private void testAppWithDataset(Class<? extends Application> app, String serviceName) throws Exception {
    ApplicationManager applicationManager = deployApplication(app);
    // Query the result
    ServiceManager serviceManager = applicationManager.getServiceManager(serviceName).start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    callServicePut(serviceManager.getServiceURL(), "key1", "value1");
    String response = callServiceGet(serviceManager.getServiceURL(), "key1");
    Assert.assertEquals("value1", new Gson().fromJson(response, String.class));
    serviceManager.stop();
    serviceManager.waitForRun(ProgramRunStatus.KILLED, 10, TimeUnit.SECONDS);
  }

  @Category(XSlowTests.class)
  @Test
  public void testByteCodeClassLoader() throws Exception {
    // This test verify bytecode generated classes ClassLoading

    ApplicationManager appManager = deployApplication(testSpace, ClassLoaderTestApp.class);

    ServiceManager serviceManager = appManager.getServiceManager("RecordHandler").start();
    URL serviceURL = serviceManager.getServiceURL(15, TimeUnit.SECONDS);
    Assert.assertNotNull(serviceURL);

    // Increment record
    URL url = new URL(serviceURL, "increment/public");
    for (int i = 0; i < 10; i++) {
      HttpResponse response = executeHttp(HttpRequest.post(url).build());
      Assert.assertEquals(200, response.getResponseCode());
    }

    // Query record
    url = new URL(serviceURL, "query?type=public");
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = executeHttp(request);
    Assert.assertEquals(200, response.getResponseCode());

    long count = Long.parseLong(response.getResponseBodyAsString());
    serviceManager.stop();

    // Verify the record count with dataset
    DataSetManager<KeyValueTable> recordsManager = getDataset(testSpace.dataset("records"));
    KeyValueTable records = recordsManager.get();
    Assert.assertEquals(count, Bytes.toLong(records.read("PUBLIC")));
  }

  @Test
  public void testConcurrentRuns() throws Exception {
    ApplicationManager appManager = deployApplication(ConcurrentRunTestApp.class);
    WorkerManager workerManager = appManager.getWorkerManager(ConcurrentRunTestApp.TestWorker.class.getSimpleName());
    workerManager.start();
    // Start another time should fail as worker doesn't support concurrent run.
    workerManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    try {
      workerManager.start();
      Assert.fail("Expected failure to start worker");
    } catch (Exception e) {
      Assert.assertTrue(Throwables.getRootCause(e) instanceof ConflictException);
    }
    workerManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    workerManager.stop();

    // Start the workflow
    File tmpDir = TEMP_FOLDER.newFolder();
    File actionFile = new File(tmpDir, "action.file");
    Map<String, String> args = Collections.singletonMap("action.file", actionFile.getAbsolutePath());
    WorkflowManager workflowManager = appManager.getWorkflowManager(
      ConcurrentRunTestApp.TestWorkflow.class.getSimpleName());

    // Starts two runs, both should succeed
    workflowManager.start(args);
    workflowManager.start(args);

    // Should get two active runs
    workflowManager.waitForRuns(ProgramRunStatus.RUNNING, 2, 10L, TimeUnit.SECONDS);

    // Touch the file to complete the workflow runs
    Files.touch(actionFile);

    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 10L, TimeUnit.SECONDS);
  }

  @Category(SlowTests.class)
  @Test
  public void testMetadataAccessInService() throws Exception {
    ApplicationManager applicationManager = deployApplication(AppWithMetadataPrograms.class);
    LOG.info("Deployed.");
    ServiceManager serviceManager =
      applicationManager.getServiceManager(AppWithMetadataPrograms.METADATA_SERVICE_NAME).start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    LOG.info("Service Started");

    URL serviceURL = serviceManager.getServiceURL(15, TimeUnit.SECONDS);
    Assert.assertNotNull(serviceURL);
    // add some tags
    callServicePut(serviceManager.getServiceURL(), "metadata/" + AppWithMetadataPrograms.METADATA_SERVICE_DATASET +
      "/tags", "tag1");
    callServicePut(serviceManager.getServiceURL(), "metadata/" + AppWithMetadataPrograms.METADATA_SERVICE_DATASET +
      "/tags", "tag2");

    // add some properties
    Map<String, String> propertiesToAdd = ImmutableMap.of("k1", "v1", "k2", "v2");
    callServicePut(serviceManager.getServiceURL(), "metadata/" + AppWithMetadataPrograms.METADATA_SERVICE_DATASET +
      "/properties", GSON.toJson(propertiesToAdd));

    // test service is able to read metadata
    String result = callServiceGet(serviceManager.getServiceURL(), "metadata/" +
      AppWithMetadataPrograms.METADATA_SERVICE_DATASET);

    Map<MetadataScope, Metadata> scopeMetadata = GSON.fromJson(result, MAP_METADATASCOPE_METADATA_TYPE);

    // verify system metadata
    Assert.assertTrue(scopeMetadata.containsKey(MetadataScope.SYSTEM));
    Assert.assertTrue(scopeMetadata.containsKey(MetadataScope.USER));
    Assert.assertFalse(scopeMetadata.get(MetadataScope.SYSTEM).getProperties().isEmpty());
    Assert.assertFalse(scopeMetadata.get(MetadataScope.SYSTEM).getTags().isEmpty());
    Assert.assertTrue(scopeMetadata.get(MetadataScope.SYSTEM).getProperties().containsKey("entity-name"));
    Assert.assertEquals(AppWithMetadataPrograms.METADATA_SERVICE_DATASET,
                        scopeMetadata.get(MetadataScope.SYSTEM).getProperties().get("entity-name"));
    Assert.assertTrue(scopeMetadata.get(MetadataScope.SYSTEM).getTags()
                        .containsAll(Arrays.asList("explore", "batch")));

    // verify user metadata
    Assert.assertFalse(scopeMetadata.get(MetadataScope.USER).getProperties().isEmpty());
    Assert.assertFalse(scopeMetadata.get(MetadataScope.USER).getTags().isEmpty());
    Assert.assertTrue(scopeMetadata.get(MetadataScope.USER).getTags().containsAll(Arrays.asList("tag1", "tag2")));
    Assert.assertTrue(scopeMetadata.get(MetadataScope.USER).getProperties().containsKey("k1"));
    Assert.assertTrue(scopeMetadata.get(MetadataScope.USER).getProperties().containsKey("k2"));
    Assert.assertEquals("v1", scopeMetadata.get(MetadataScope.USER).getProperties().get("k1"));
    Assert.assertEquals("v2", scopeMetadata.get(MetadataScope.USER).getProperties().get("k2"));

    // delete a tag
    callServiceDelete(serviceManager.getServiceURL(), "metadata/" + AppWithMetadataPrograms.METADATA_SERVICE_DATASET
      + "/tags/" + "tag1");

    // delete a property
    callServiceDelete(serviceManager.getServiceURL(), "metadata/" + AppWithMetadataPrograms.METADATA_SERVICE_DATASET
      + "/properties/" + "k1");

    // get metadata and verify
    result = callServiceGet(serviceManager.getServiceURL(), "metadata/" +
      AppWithMetadataPrograms.METADATA_SERVICE_DATASET);
    scopeMetadata = GSON.fromJson(result, MAP_METADATASCOPE_METADATA_TYPE);
    Assert.assertEquals(1, scopeMetadata.get(MetadataScope.USER).getTags().size());
    Assert.assertTrue(scopeMetadata.get(MetadataScope.USER).getTags().contains("tag2"));
    Assert.assertEquals(1, scopeMetadata.get(MetadataScope.USER).getProperties().size());
    Assert.assertTrue(scopeMetadata.get(MetadataScope.USER).getProperties().containsKey("k2"));
    Assert.assertEquals("v2", scopeMetadata.get(MetadataScope.USER).getProperties().get("k2"));

    // delete all tags
    callServiceDelete(serviceManager.getServiceURL(), "metadata/" + AppWithMetadataPrograms.METADATA_SERVICE_DATASET
      + "/tags");

    // get metadata and verify
    result = callServiceGet(serviceManager.getServiceURL(), "metadata/" +
      AppWithMetadataPrograms.METADATA_SERVICE_DATASET);
    scopeMetadata = GSON.fromJson(result, MAP_METADATASCOPE_METADATA_TYPE);
    Assert.assertTrue(scopeMetadata.get(MetadataScope.USER).getTags().isEmpty());
    Assert.assertFalse(scopeMetadata.get(MetadataScope.USER).getProperties().isEmpty());

    // delete all properties
    callServiceDelete(serviceManager.getServiceURL(), "metadata/" + AppWithMetadataPrograms.METADATA_SERVICE_DATASET
      + "/properties");

    // get metadata and verify
    result = callServiceGet(serviceManager.getServiceURL(), "metadata/" +
      AppWithMetadataPrograms.METADATA_SERVICE_DATASET);
    scopeMetadata = GSON.fromJson(result, MAP_METADATASCOPE_METADATA_TYPE);
    Assert.assertTrue(scopeMetadata.get(MetadataScope.USER).getProperties().isEmpty());

    // add some tag and property again
    callServicePut(serviceManager.getServiceURL(), "metadata/" + AppWithMetadataPrograms.METADATA_SERVICE_DATASET +
      "/tags", "tag1");
    callServicePut(serviceManager.getServiceURL(), "metadata/" + AppWithMetadataPrograms.METADATA_SERVICE_DATASET +
      "/properties", GSON.toJson(propertiesToAdd));

    // get metadata and verify
    result = callServiceGet(serviceManager.getServiceURL(), "metadata/" +
      AppWithMetadataPrograms.METADATA_SERVICE_DATASET);
    scopeMetadata = GSON.fromJson(result, MAP_METADATASCOPE_METADATA_TYPE);
    Assert.assertFalse(scopeMetadata.get(MetadataScope.USER).getTags().isEmpty());
    Assert.assertFalse(scopeMetadata.get(MetadataScope.USER).getProperties().isEmpty());

    // delete all metadata
    callServiceDelete(serviceManager.getServiceURL(), "metadata/" + AppWithMetadataPrograms.METADATA_SERVICE_DATASET);

    // get metadata and verify
    result = callServiceGet(serviceManager.getServiceURL(), "metadata/" +
      AppWithMetadataPrograms.METADATA_SERVICE_DATASET);
    scopeMetadata = GSON.fromJson(result, MAP_METADATASCOPE_METADATA_TYPE);
    Assert.assertTrue(scopeMetadata.get(MetadataScope.USER).getTags().isEmpty());
    Assert.assertTrue(scopeMetadata.get(MetadataScope.USER).getProperties().isEmpty());
  }

  private int callServiceGetResponseCode(URL serviceURL, String path) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL(serviceURL.toString() + path).openConnection();
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private String callServiceGet(URL serviceURL, String path) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL(serviceURL.toString() + path).openConnection();
    try {
      int responseCode = connection.getResponseCode();

      if (responseCode != 200) {
        try (InputStream errStream = connection.getErrorStream()) {
          String error = CharStreams.toString(new InputStreamReader(errStream, StandardCharsets.UTF_8));
          throw new IOException("Error response " + responseCode + " from " + serviceURL + ": " + error);
        }
      }

      try (InputStream in = connection.getInputStream()) {
        return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)).readLine();
      }
    } finally {
      connection.disconnect();
    }
  }

  private String callServicePut(URL serviceURL, String path, String body) throws Exception {
    return callServicePut(serviceURL, path, body, 200);
  }

  @Nullable
  private String callServicePut(URL serviceURL, String path, String body, Integer expectedStatus) throws Exception {
    return callServicePut(serviceURL, path, body, expectedStatus, null);
  }

  @Nullable
  private String callServicePut(URL serviceURL, String path, String body, Integer expectedStatus,
                                String doneFilePath) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) new URL(serviceURL.toString() + path).openConnection();
    try {
      connection.setDoOutput(true);
      connection.setRequestMethod("PUT");
      try (OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream())) {
        out.write(body);
      }
      int expected = expectedStatus == null ? 200 : expectedStatus;
      Assert.assertEquals(expected, connection.getResponseCode());
      if (expectedStatus != null) {
        return null;
      }
      try (InputStream in = connection.getInputStream()) {
        return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)).readLine();
      }
    } finally {
      if (doneFilePath != null) {
        File doneFile = new File(doneFilePath);
        LOG.info("Checking for doneFilePath: {}", doneFilePath);
        Tasks.waitFor(true, doneFile::exists, 30, TimeUnit.SECONDS);
      }
      connection.disconnect();
    }
  }

  private void callServiceDelete(URL serviceURL, String path) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL(serviceURL.toString() + path).openConnection();
    try {
      connection.setRequestMethod("DELETE");
      int responseCode = connection.getResponseCode();

      if (responseCode != 200) {
        try (InputStream errStream = connection.getErrorStream()) {
          String error = CharStreams.toString(new InputStreamReader(errStream, StandardCharsets.UTF_8));
          throw new IOException("Error response " + responseCode + " from " + serviceURL + ": " + error);
        }
      }
    } finally {
      connection.disconnect();
    }
  }
}
