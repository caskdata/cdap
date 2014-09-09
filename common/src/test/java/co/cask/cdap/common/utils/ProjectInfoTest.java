/*
 * Copyright 2014 Cask Data, Inc.
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
package co.cask.cdap.common.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for testing {@link ProjectInfo}.
 */
public class ProjectInfoTest {

  @Test
  public void testInfo() {
    Assert.assertTrue(ProjectInfo.getVersion().getMajor() > 0);
    Assert.assertTrue(ProjectInfo.getVersion().getBuildTime() > 0L);
  }

  @Test
  public void testVersion() {
    ProjectInfo.Version version = new ProjectInfo.Version("2.1.0-SNAPSHOT-12345");
    Assert.assertEquals(2, version.getMajor());
    Assert.assertEquals(1, version.getMinor());
    Assert.assertEquals(0, version.getFix());
    Assert.assertTrue(version.isSnapshot());
    Assert.assertEquals(12345L, version.getBuildTime());

    Assert.assertEquals("2.1.0-SNAPSHOT-12345", version.toString());
  }

  @Test
  public void testVersionCompare() {
    // Major version
    ProjectInfo.Version version1 = new ProjectInfo.Version("2.1.0-SNAPSHOT-12345");
    ProjectInfo.Version version2 = new ProjectInfo.Version("3.0.0-SNAPSHOT-12345");

    Assert.assertTrue(version1.compareTo(version1) == 0);
    Assert.assertTrue(version1.compareTo(version2) < 0);
    Assert.assertTrue(version2.compareTo(version1) > 0);

    // Minor version
    version1 = new ProjectInfo.Version("2.0.0-SNAPSHOT-12345");
    version2 = new ProjectInfo.Version("2.1.0-SNAPSHOT-12345");

    Assert.assertTrue(version1.compareTo(version1) == 0);
    Assert.assertTrue(version1.compareTo(version2) < 0);
    Assert.assertTrue(version2.compareTo(version1) > 0);

    // Fix version
    version1 = new ProjectInfo.Version("2.1.0-SNAPSHOT-12345");
    version2 = new ProjectInfo.Version("2.1.1-SNAPSHOT-12345");

    Assert.assertTrue(version1.compareTo(version1) == 0);
    Assert.assertTrue(version1.compareTo(version2) < 0);
    Assert.assertTrue(version2.compareTo(version1) > 0);

    // Snapshot, non-snapshot
    version1 = new ProjectInfo.Version("2.1.0-SNAPSHOT-12345");
    version2 = new ProjectInfo.Version("2.1.0-12345");

    Assert.assertTrue(version1.compareTo(version1) == 0);
    Assert.assertTrue(version1.compareTo(version2) < 0);
    Assert.assertTrue(version2.compareTo(version1) > 0);

    // Buildtime
    version1 = new ProjectInfo.Version("2.1.0-12345");
    version2 = new ProjectInfo.Version("2.1.0-12346");

    Assert.assertTrue(version1.compareTo(version1) == 0);
    Assert.assertTrue(version1.compareTo(version2) < 0);
    Assert.assertTrue(version2.compareTo(version1) > 0);

    // Buildtime with snapshot
    version1 = new ProjectInfo.Version("2.1.0-SNAPSHOT-12345");
    version2 = new ProjectInfo.Version("2.1.0-SNAPSHOT-12346");

    Assert.assertTrue(version1.compareTo(version1) == 0);
    Assert.assertTrue(version1.compareTo(version2) < 0);
    Assert.assertTrue(version2.compareTo(version1) > 0);
  }
}
