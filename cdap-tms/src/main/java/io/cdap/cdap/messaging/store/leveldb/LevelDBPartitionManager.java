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

import io.cdap.cdap.common.utils.DirUtils;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * Manages partitions of a logical MessageTable.
 * Each partition is a physical LevelDB table in a directory structure like:
 *
 *   [base dir]/v2.[namespace].[tablename].[topic].[generation]/part.[start].[end]
 *
 * Each partition contains messages with a publish time between the start (inclusive) and end (exclusive) timestamps.
 */
public class LevelDBPartitionManager implements Closeable {
  private static final Iq80DBFactory LEVEL_DB_FACTORY = Iq80DBFactory.factory;
  private static final String PART_PREFIX = "part.";
  private final File topicDir;
  private final Options dbOptions;
  private final long partitionSizeMillis;
  private final ConcurrentNavigableMap<Long, LevelDBPartition> partitions;
  private final AtomicBoolean initialized;

  public LevelDBPartitionManager(File topicDir, Options dbOptions, long partitionSizeMillis) {
    this.topicDir = topicDir;
    this.dbOptions = dbOptions;
    this.partitionSizeMillis = partitionSizeMillis;
    this.partitions = new ConcurrentSkipListMap<>();
    this.initialized = new AtomicBoolean(false);
  }

  /**
   * Prune partitions that have an end timestamp older than or equal to the given threshold timestamp.
   */
  public int prunePartitions(long thresholdTimestamp) throws IOException {
    File[] partitionDirs = topicDir.listFiles();
    int numPruned = 0;
    if (partitionDirs == null) {
      return numPruned;
    }
    for (File partitionDir : partitionDirs) {
      String partitionName = partitionDir.getName();
      TimeInterval timeInterval = extractIntervalFromName(partitionName);
      if (timeInterval == null) {
        // can only happen if somebody manually created some files/directories here
        continue;
      }
      if (thresholdTimestamp >= timeInterval.endMillis) {
        LevelDBPartition partition = partitions.remove(timeInterval.startMillis);
        if (partition != null) {
          partition.close();
          DirUtils.deleteDirectoryContents(partitionDir);
        }
        numPruned++;
      }
    }
    return numPruned;
  }

  public LevelDBPartition getOrCreatePartition(long publishTime) throws IOException {
    LevelDBPartition partition = getPartitionIfExists(publishTime);
    if (partition != null) {
      return partition;
    }

    synchronized (this) {
      // Check again to make sure no new instance was being created while this thread is acquiring the lock
      partition = getPartitionIfExists(publishTime);
      if (partition != null) {
        return partition;
      }

      partition = getOrCreateDBPartition(publishTime);
      partitions.put(partition.getStartTime(), partition);

      return partition;
    }
  }

  /**
   * Get existing partitions for publish times equal to or greater than the given timestamp.
   */
  public Collection<LevelDBPartition> getPartitions(long startTime) throws IOException {
    if (initialized.compareAndSet(false, true)) {
      initPartitions();
    }
    Long partitionStartTime = partitions.floorKey(startTime);
    return partitionStartTime == null ? partitions.values() : partitions.tailMap(partitionStartTime, true).values();
  }

  /**
   * Get the partitions responsible for data between the specified start and end times.
   *
   * @param startTime inclusive start time
   * @param endTime inclusive end time
   * @return partitions responsible for data between the specified start and end times
   */
  public Collection<LevelDBPartition> getPartitions(long startTime, long endTime) throws IOException {
    if (initialized.compareAndSet(false, true)) {
      initPartitions();
    }
    Long partitionEndTime = partitions.floorKey(endTime);
    if (partitionEndTime == null) {
      // partitionEndTime can only be null if the partitions map is empty, or the endTime is less than the
      // start time for any existing partition. In either case, there are no partitions that contain data
      // published between the start and end times.
      return Collections.emptyList();
    }

    Long partitionStartTime = partitions.floorKey(startTime);
    // partitionStartTime can be null if the startTime is less than the start time for any existing partition
    partitionStartTime = partitionStartTime == null ? 0 : partitionStartTime;
    return partitions.subMap(partitionStartTime, true, partitionEndTime, true).values();
  }

  public void close() throws IOException {
    List<LevelDBPartition> partitionsToClose;

    synchronized (this) {
      partitionsToClose = new ArrayList<>(partitions.values());
      partitions.clear();
    }

    IOException failure = null;
    for (LevelDBPartition partition : partitionsToClose) {
      try {
        partition.close();
      } catch (IOException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  @Nullable
  private LevelDBPartition getPartitionIfExists(long publishTime) {
    Map.Entry<Long, LevelDBPartition> partitionEntry = partitions.floorEntry(publishTime);
    if (partitionEntry != null) {
      LevelDBPartition partition = partitionEntry.getValue();
      if (partition.getEndTime() > publishTime) {
        return partition;
      }
    }
    return null;
  }

  private synchronized void initPartitions() throws IOException {
    ensureDirExists(topicDir);

    for (File partitionDir : DirUtils.listFiles(topicDir)) {
      if (!partitionDir.isDirectory()) {
        continue;
      }

      String dirName = partitionDir.getName();
      TimeInterval interval = extractIntervalFromName(dirName);
      if (interval == null) {
        // should not happen unless somebody manually created a directory here
        continue;
      }
      LevelDBPartition partition = new LevelDBPartition(partitionDir, interval.startMillis, interval.endMillis,
                                                        () -> LEVEL_DB_FACTORY.open(partitionDir, dbOptions));
      partitions.put(partition.getStartTime(), partition);
    }
  }

  private LevelDBPartition getOrCreateDBPartition(long publishTime) throws IOException {
    if (initialized.compareAndSet(false, true)) {
      initPartitions();
    }

    long closestEndTime = Long.MAX_VALUE;
    long closestStartTime = Long.MAX_VALUE;
    for (LevelDBPartition partition : partitions.values()) {
      if (publishTime >= partition.getStartTime() && publishTime < partition.getEndTime()) {
        // if the publish time is within the time range, return the partition
        return partition;
      }

      long startTimeDiff = Math.abs(partition.getStartTime() - publishTime);
      long endTimeDiff = Math.abs(partition.getEndTime() - publishTime);
      if (Math.abs(closestStartTime - publishTime) > startTimeDiff) {
        closestStartTime = partition.getStartTime();
      }
      if (Math.abs(closestEndTime - publishTime) > endTimeDiff) {
        closestEndTime = partition.getEndTime();
      }
    }

    // If there are no existing partitions, create a new one from [0 -> publishtime + partition size)
    if (partitions.isEmpty()) {
      return createPartition(topicDir, 0, publishTime + partitionSizeMillis);
    }

    /*
        If partitions exist but the publish time is not within an existing range,
        determine the time range of the new partition.

        If the publish time is before all existing partitions, create a new partition from [0, closestStartTime)

        If the publish time is after all existing partitions,
        create a new partition from [closestEndTime, max(closestEndTime + partitionSize, publishTime))

        Otherwise, the publish time is in the middle of two existing partitions, and the new partition
        will be one that fills in the gap from [closestStartTime, closestEndTime)

        For example, suppose partitions size is 1000 and existing partitions are:

          [2000 -> 3000), [3000 -> 4000), [6000 -> 7000)

        If the publish time is 1950, the new partition will be [0, 2000)
        If the publish time is 7100, the new partition will be [7000, 8000)
        If the publish time is 8100, the new partition will be [7000, 8100)
        If the publish time is 4500, the new partition will be [4000, 6000)
     */
    if (publishTime < closestStartTime) {
      return createPartition(topicDir, 0, closestStartTime);
    }
    if (publishTime >= closestEndTime) {
      return createPartition(topicDir, closestEndTime, Math.max(closestEndTime + partitionSizeMillis, publishTime));
    }
    return createPartition(topicDir, closestStartTime, closestEndTime);
  }

  static File getPartitionDir(File topicDir, long start, long end) {
    return new File(topicDir, String.format("%s%d.%d", PART_PREFIX, start, end));
  }

  private LevelDBPartition createPartition(File topicDir, long start, long end) throws IOException {
    File dbPath = getPartitionDir(topicDir, start, end);
    ensureDirExists(dbPath);
    return new LevelDBPartition(dbPath, start, end, () -> LEVEL_DB_FACTORY.open(dbPath, dbOptions));
  }

  private File ensureDirExists(File dir) throws IOException {
    if (!DirUtils.mkdirs(dir)) {
      throw new IOException("Failed to create local directory " + dir + " for the messaging system.");
    }
    return dir;
  }

  private static class TimeInterval {
    private final long startMillis;
    private final long endMillis;

    private TimeInterval(long startMillis, long endMillis) {
      this.startMillis = startMillis;
      this.endMillis = endMillis;
    }
  }

  @Nullable
  static TimeInterval extractIntervalFromName(String name) {
    // should always parse properly unless somebody manually created directories here
    if (!name.startsWith(PART_PREFIX)) {
      return null;
    }
    int dotIdx = name.lastIndexOf('.');
    if (dotIdx < 0 || dotIdx == name.length() - 1) {
      return null;
    }
    String endTimeStr = name.substring(dotIdx + 1);
    String startTimeStr = name.substring(PART_PREFIX.length(), dotIdx);
    try {
      long endTime = Long.parseLong(endTimeStr);
      long startTime = Long.parseLong(startTimeStr);
      return new TimeInterval(startTime, endTime);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
