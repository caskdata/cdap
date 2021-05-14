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

import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * An iterator that scans over multiple partitions of a MessageTable.
 *
 * @param <T> type of object to scan
 */
public class PartitionedDBScanIterator<T> extends AbstractCloseableIterator<T> {
  private final Iterator<LevelDBPartition> partitionIter;
  private final byte[] startRow;
  private final byte[] stopRow;
  private final BiFunction<byte[], byte[], T> decodeFunction;
  private boolean closed;
  private CloseableIterator<Map.Entry<byte[], byte[]>> currentPartition;

  public PartitionedDBScanIterator(Iterator<LevelDBPartition> partitionIter, byte[] startRow, byte[] stopRow,
                                   BiFunction<byte[], byte[], T> decodeFunction) throws IOException {
    this.partitionIter = partitionIter;
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.decodeFunction = decodeFunction;
    this.closed = false;
    this.currentPartition = partitionIter.hasNext() ?
      new DBScanIterator(partitionIter.next().getLevelDB(), startRow, stopRow) : CloseableIterator.empty();
  }

  @Override
  protected T computeNext() {
    if (closed) {
      return endOfData();
    }

    if (!currentPartition.hasNext()) {
      while (partitionIter.hasNext()) {
        try {
          currentPartition = new DBScanIterator(partitionIter.next().getLevelDB(), startRow, stopRow);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        if (currentPartition.hasNext()) {
          break;
        }
      }
    }

    if (!currentPartition.hasNext()) {
      return endOfData();
    }

    Map.Entry<byte[], byte[]> row = currentPartition.next();
    return decodeFunction.apply(row.getKey(), row.getValue());
  }

  @Override
  public void close() {
    try {
      currentPartition.close();
    } finally {
      endOfData();
      closed = true;
    }
  }
}
