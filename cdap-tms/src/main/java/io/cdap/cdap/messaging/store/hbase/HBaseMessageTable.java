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

package io.cdap.cdap.messaging.store.hbase;

import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.data2.util.hbase.PutBuilder;
import io.cdap.cdap.hbase.wd.AbstractRowKeyDistributor;
import io.cdap.cdap.hbase.wd.DistributedScanner;
import io.cdap.cdap.messaging.MessagingUtils;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.store.AbstractMessageTable;
import io.cdap.cdap.messaging.store.MessageTable;
import io.cdap.cdap.messaging.store.MessageTableKey;
import io.cdap.cdap.messaging.store.RawMessageTableEntry;
import io.cdap.cdap.messaging.store.RollbackRequest;
import io.cdap.cdap.messaging.store.ScanRequest;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * HBase implementation of {@link MessageTable}.
 */
final class HBaseMessageTable extends AbstractMessageTable {
  private static final byte[] PAYLOAD_COL = MessagingUtils.Constants.PAYLOAD_COL;
  private static final byte[] TX_COL = MessagingUtils.Constants.TX_COL;

  private final HBaseTableUtil tableUtil;
  private final byte[] columnFamily;
  private final Table table;
  private final BufferedMutator mutator;
  private final AbstractRowKeyDistributor rowKeyDistributor;
  private final ExecutorService scanExecutor;
  private final int scanCacheRows;
  private final HBaseExceptionHandler exceptionHandler;

  HBaseMessageTable(HBaseTableUtil tableUtil, Table table, byte[] columnFamily,
                    AbstractRowKeyDistributor rowKeyDistributor, ExecutorService scanExecutor,
                    int scanCacheRows, HBaseExceptionHandler exceptionHandler) throws IOException {
    this.tableUtil = tableUtil;
    this.table = table;
    this.mutator = tableUtil.createBufferedMutator(table, HBaseTableUtil.DEFAULT_WRITE_BUFFER_SIZE);
    this.columnFamily = Arrays.copyOf(columnFamily, columnFamily.length);
    this.rowKeyDistributor = rowKeyDistributor;
    this.scanExecutor = scanExecutor;
    this.scanCacheRows = scanCacheRows;
    this.exceptionHandler = exceptionHandler;
  }

  @Override
  protected CloseableIterator<RawMessageTableEntry> scan(ScanRequest scanRequest) throws IOException {
    Scan scan = tableUtil.buildScan()
      .setStartRow(scanRequest.getStartRow())
      .setStopRow(scanRequest.getStopRow())
      .setCaching(scanCacheRows)
      .build();

    TopicMetadata topicMetadata = scanRequest.getTopicMetadata();
    byte[] topic = MessagingUtils.toDataKeyPrefix(topicMetadata.getTopicId(), topicMetadata.getGeneration());
    MessageTableKey messageTableKey = MessageTableKey.fromTopic(topic);
    try {
      final ResultScanner scanner = DistributedScanner.create(table, scan, rowKeyDistributor, scanExecutor);
      final RawMessageTableEntry tableEntry = new RawMessageTableEntry();
      return new AbstractCloseableIterator<RawMessageTableEntry>() {
        private boolean closed = false;

        @Override
        protected RawMessageTableEntry computeNext() {
          if (closed) {
            return endOfData();
          }

          Result result;
          try {
            result = scanner.next();
          } catch (IOException e) {
            throw exceptionHandler.handleAndWrap(e);
          }
          if (result == null) {
            return endOfData();
          }

          byte[] originalKey = rowKeyDistributor.getOriginalKey(result.getRow());
          messageTableKey.setFromRowKey(originalKey);
          return tableEntry.set(messageTableKey,
                                result.getValue(columnFamily, TX_COL),
                                result.getValue(columnFamily, PAYLOAD_COL));
        }

        @Override
        public void close() {
          try {
            scanner.close();
          } finally {
            endOfData();
            closed = true;
          }
        }
      };
    } catch (IOException e) {
      throw exceptionHandler.handle(e);
    }
  }

  @Override
  protected void persist(Iterator<RawMessageTableEntry> entries) throws IOException {
    List<Put> batchPuts = new ArrayList<>();
    while (entries.hasNext()) {
      RawMessageTableEntry entry = entries.next();
      PutBuilder putBuilder = tableUtil.buildPut(rowKeyDistributor.getDistributedKey(entry.getKey().getRowKey()));
      if (entry.getTxPtr() != null) {
        putBuilder.add(columnFamily, TX_COL, entry.getTxPtr());
      }

      if (entry.getPayload() != null) {
        putBuilder.add(columnFamily, PAYLOAD_COL, entry.getPayload());
      }
      batchPuts.add(putBuilder.build());
    }

    try {
      if (!batchPuts.isEmpty()) {
        mutator.mutate(batchPuts);
        mutator.flush();
      }
    } catch (IOException e) {
      throw exceptionHandler.handle(e);
    }
  }

  @Override
  public void rollback(RollbackRequest rollbackRequest) throws IOException {
    Scan scan = tableUtil.buildScan()
      .setStartRow(rollbackRequest.getStartRow())
      .setStopRow(rollbackRequest.getStopRow())
      .setCaching(scanCacheRows)
      .build();

    List<Put> batchPuts = new ArrayList<>();
    try (ResultScanner scanner = DistributedScanner.create(table, scan, rowKeyDistributor, scanExecutor)) {
      for (Result result : scanner) {
        // No need to turn the key back to the original row key because we want to put with the actual row key
        PutBuilder putBuilder = tableUtil.buildPut(result.getRow());
        putBuilder.add(columnFamily, TX_COL, rollbackRequest.getTxWritePointer());
        batchPuts.add(putBuilder.build());
      }
    }

    try {
      if (!batchPuts.isEmpty()) {
        mutator.mutate(batchPuts);
        mutator.flush();
      }
    } catch (IOException e) {
      throw exceptionHandler.handle(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      mutator.close();
    } finally {
      table.close();
    }
  }
}
