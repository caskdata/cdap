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

import org.iq80.leveldb.DB;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * One partition of a MessageTable, backed by a LevelDB table.
 */
public class LevelDBPartition implements Closeable {
  private final File file;
  private final long startTime;
  private final long endTime;
  private final Supplier levelDBSupplier;
  private volatile DB levelDB;

  public LevelDBPartition(File file, long startTime, long endTime, Supplier levelDBSupplier) {
    this.file = file;
    this.startTime = startTime;
    this.endTime = endTime;
    this.levelDBSupplier = levelDBSupplier;
  }

  public File getFile() {
    return file;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public DB getLevelDB() throws IOException {
    if (levelDB != null) {
      return levelDB;
    }
    synchronized (this) {
      if (levelDB == null) {
        levelDB = levelDBSupplier.get();
      }
    }
    return levelDB;
  }

  @Override
  public void close() throws IOException {
    if (levelDB != null) {
      levelDB.close();
    }
  }

  /**
   * Supplies an opened LevelDB
   */
  public interface Supplier {
    DB get() throws IOException;
  }
}
