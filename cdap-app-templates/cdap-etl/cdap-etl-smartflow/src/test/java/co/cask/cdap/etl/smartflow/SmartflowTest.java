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

package co.cask.cdap.etl.smartflow;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.smartflow.mock.IdentityTransform;
import co.cask.cdap.etl.smartflow.mock.MockSink;
import co.cask.cdap.etl.smartflow.mock.MockSource;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SmartflowTest extends TestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final Id.Artifact APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT, "smartflow", "3.4.0");
  protected static final ArtifactSummary ETLBATCH_ARTIFACT = new ArtifactSummary("smartflow", "3.4.0");

  private static int startCount;

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    // add the artifact for etl batch app
    addAppArtifact(APP_ARTIFACT_ID, SmartflowApp.class,
                   BatchSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    // add some test plugins
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "test-plugins", "1.0.0"), APP_ARTIFACT_ID,
                      MockSource.class, MockSink.class, IdentityTransform.class);
  }

  @Test
  public void testMultiSource() throws Exception {
    /*
     * source1 --|                 |--> sink1
     *           |--> transform1 --|
     * source2 --|                 |
     *                             |--> transform2 --> sink2
     * source3 --------------------|
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source1", MockSource.getPlugin("msInput1")))
      .addStage(new ETLStage("source2", MockSource.getPlugin("msInput2")))
      .addStage(new ETLStage("source3", MockSource.getPlugin("msInput3")))
      .addStage(new ETLStage("transform1", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("transform2", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("sink1", MockSink.getPlugin("msOutput1")))
      .addStage(new ETLStage("sink2", MockSink.getPlugin("msOutput2")))
      .addConnection("source1", "transform1")
      .addConnection("source2", "transform1")
      .addConnection("transform1", "sink1")
      .addConnection("transform1", "transform2")
      .addConnection("transform2", "sink2")
      .addConnection("source3", "transform2")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "MultiSourceApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(schema).set("name", "jane").build();

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, "msInput1");
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel));
    inputManager = getDataset(Id.Namespace.DEFAULT, "msInput2");
    MockSource.writeInput(inputManager, ImmutableList.of(recordBob));
    inputManager = getDataset(Id.Namespace.DEFAULT, "msInput3");
    MockSource.writeInput(inputManager, ImmutableList.of(recordJane));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    // sink1 should get records from source1 and source2
    DataSetManager<Table> sinkManager = getDataset("msOutput1");
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    // sink2 should get all records
    sinkManager = getDataset("msOutput2");
    expected = ImmutableSet.of(recordSamuel, recordBob, recordJane);
    actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);
  }
}
