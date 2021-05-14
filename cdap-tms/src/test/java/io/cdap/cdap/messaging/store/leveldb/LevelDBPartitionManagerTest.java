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

package io.cdap.cdap.messaging.store.leveldb;

import org.iq80.leveldb.Options;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Tests for {@link LevelDBPartitionManager}.
 */
public class LevelDBPartitionManagerTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Options DB_OPTIONS = new Options()
    .errorIfExists(false)
    .createIfMissing(true);

  @Test
  public void testPrunePartitions() throws IOException {
    LevelDBPartitionManager partitionManager = new LevelDBPartitionManager(tmpFolder.newFolder(), DB_OPTIONS, 1000);

    // create 3 partitions:
    //   partition1: [0, 1000)
    //   partition2: [1000, 2000)
    //   partition3: [2000, 3000)
    partitionManager.getOrCreatePartition(0);
    partitionManager.getOrCreatePartition(1000);
    partitionManager.getOrCreatePartition(2000);
    Assert.assertEquals(3, partitionManager.getPartitions(0).size());

    // should prune partition1 since 1000 is older than [0, 1000), but not partition2 or partition3
    Assert.assertEquals(1, partitionManager.prunePartitions(1000));
    Assert.assertEquals(new HashSet<>(Arrays.asList(1000L, 2000L)),
                        partitionManager.getPartitions(0).stream()
                          .map(LevelDBPartition::getStartTime)
                          .collect(Collectors.toSet()));
    // should not prune anything
    Assert.assertEquals(0, partitionManager.prunePartitions(1001));
    // should prune rest of partitions
    Assert.assertEquals(2, partitionManager.prunePartitions(3000));
    Assert.assertEquals(0, partitionManager.getPartitions(0).size());
  }

  @Test
  public void testCreatePartitions() throws IOException {
    LevelDBPartitionManager partitionManager = new LevelDBPartitionManager(tmpFolder.newFolder(), DB_OPTIONS, 1000);

    // check that there are no partitions
    Assert.assertTrue(partitionManager.getPartitions(0).isEmpty());

    // first partition should be from [0, publish time + partition size)
    LevelDBPartition partition = partitionManager.getOrCreatePartition(5000);
    Assert.assertEquals(0, partition.getStartTime());
    Assert.assertEquals(6000, partition.getEndTime());

    // should not create new partitions for anything within [0,6000)
    for (long i = 0; i < 6000; i += 500) {
      partition = partitionManager.getOrCreatePartition(i);
      Assert.assertEquals(0, partition.getStartTime());
      Assert.assertEquals(6000, partition.getEndTime());
    }

    // new partition for publish time 7500 should fill in the gap to create [6000,7500)
    partition = partitionManager.getOrCreatePartition(7500);
    Assert.assertEquals(6000, partition.getStartTime());
    Assert.assertEquals(7500, partition.getEndTime());

    // new partition for publish time 8000 should be [7500,8500)
    partition = partitionManager.getOrCreatePartition(7500);
    Assert.assertEquals(7500, partition.getStartTime());
    Assert.assertEquals(8500, partition.getEndTime());

    // prune the [0,6000) and [6000,7500) partitions
    Assert.assertEquals(2, partitionManager.prunePartitions(8000));
    // adding new partition for 4000 should fill in the gap and create [0,7500)
    partition = partitionManager.getOrCreatePartition(4000);
    Assert.assertEquals(0, partition.getStartTime());
    Assert.assertEquals(7500, partition.getEndTime());
  }

  @Test
  public void testGetPartitions() throws IOException {
    LevelDBPartitionManager partitionManager = new LevelDBPartitionManager(tmpFolder.newFolder(), DB_OPTIONS, 1000);

    // create partitions [0,1000), [1000,2000), [2000,5000), [5000,8000)
    partitionManager.getOrCreatePartition(0);
    partitionManager.getOrCreatePartition(2000);
    partitionManager.getOrCreatePartition(5000);
    partitionManager.getOrCreatePartition(8000);

    // test getPartitions(startTime)
    Map<Long, Long> expectedIntervals = new HashMap<>();
    expectedIntervals.put(0L, 1000L);
    expectedIntervals.put(1000L, 2000L);
    expectedIntervals.put(2000L, 5000L);
    expectedIntervals.put(5000L, 8000L);
    Collection<LevelDBPartition> partitions = partitionManager.getPartitions(0);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));

    partitions = partitionManager.getPartitions(500);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));

    expectedIntervals.remove(0L);
    partitions = partitionManager.getPartitions(1800);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));

    expectedIntervals.remove(1000L);
    partitions = partitionManager.getPartitions(3333);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));

    expectedIntervals.remove(2000L);
    partitions = partitionManager.getPartitions(5555);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));

    // test getPartition(startTime, endTime)
    expectedIntervals = new HashMap<>();
    expectedIntervals.put(0L, 1000L);
    expectedIntervals.put(1000L, 2000L);
    expectedIntervals.put(2000L, 5000L);
    expectedIntervals.put(5000L, 8000L);
    partitions = partitionManager.getPartitions(0, Long.MAX_VALUE);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));
    partitions = partitionManager.getPartitions(777, Long.MAX_VALUE);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));
    partitions = partitionManager.getPartitions(3, 5000);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));

    expectedIntervals.remove(0L);
    partitions = partitionManager.getPartitions(1000, 5000);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));
    partitions = partitionManager.getPartitions(1000, 5000);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));
    partitions = partitionManager.getPartitions(1999, 9999);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));

    expectedIntervals.remove(5000L);
    partitions = partitionManager.getPartitions(1000, 4999);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));
    partitions = partitionManager.getPartitions(1337, 4999);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));
    partitions = partitionManager.getPartitions(1999, 2000);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));

    expectedIntervals.remove(2000L);
    partitions = partitionManager.getPartitions(1000, 1999);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));
    partitions = partitionManager.getPartitions(1000, 1001);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));
    partitions = partitionManager.getPartitions(1000, 1000);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));

    // prune [0,1000) partition
    Assert.assertEquals(1, partitionManager.prunePartitions(1000));

    // test getPartition(0) returns all existing partitions, even though 0 is before all existing partition start times
    expectedIntervals.put(1000L, 2000L);
    expectedIntervals.put(2000L, 5000L);
    expectedIntervals.put(5000L, 8000L);
    partitions = partitionManager.getPartitions(0);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));
    partitions = partitionManager.getPartitions(0, 10000);
    Assert.assertEquals(expectedIntervals, convertToIntervals(partitions));
  }

  @Test
  public void testIsolation() throws IOException {
    // test tables are isolated based on the directory given
    LevelDBPartitionManager manager1 = new LevelDBPartitionManager(tmpFolder.newFolder(), DB_OPTIONS, 1000);
    manager1.getOrCreatePartition(0);
    LevelDBPartitionManager manager2 = new LevelDBPartitionManager(tmpFolder.newFolder(), DB_OPTIONS, 1000);
    manager2.getOrCreatePartition(1000);

    Assert.assertEquals(1, manager1.getPartitions(0).size());
    Assert.assertEquals(1000L, manager1.getPartitions(0).iterator().next().getEndTime());

    Assert.assertEquals(1, manager2.getPartitions(0).size());
    Assert.assertEquals(2000L, manager2.getPartitions(0).iterator().next().getEndTime());
  }

  private Map<Long, Long> convertToIntervals(Collection<LevelDBPartition> partitions) {
    return partitions.stream().collect(Collectors.toMap(LevelDBPartition::getStartTime, LevelDBPartition::getEndTime));
  }
}
