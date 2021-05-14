/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.messaging.MessagingUtils;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.store.AbstractMessageTable;
import io.cdap.cdap.messaging.store.MessageTable;
import io.cdap.cdap.messaging.store.MessageTableKey;
import io.cdap.cdap.messaging.store.RawMessageTableEntry;
import io.cdap.cdap.messaging.store.RollbackRequest;
import io.cdap.cdap.messaging.store.ScanRequest;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

/**
 * LevelDB implementation of {@link MessageTable}.
 *
 * This "Table" is actually partitioned into multiple LevelDB tables. Each underlying table stores
 * messages published within a specific time range. Table names are of the form:
 *
 *   [namespace].[topic].[generation].[publish start (inclusive)][publish end (exclusive)]
 *
 * When a write occurs, messages are written to the correct table based on the their publish timestamp.
 * When a read is performed, the start and end timestamps are examined to determine if multiple underlying
 * levelDB tables need to be read.
 */
final class LevelDBMessageTable extends AbstractMessageTable {
  private static final WriteOptions WRITE_OPTIONS = new WriteOptions().sync(true);
  private static final String PAYLOAD_COL = "p";
  private static final String TX_COL = "t";

  private enum EncodeType {
    NON_TRANSACTIONAL(0),
    TRANSACTIONAL(1),
    PAYLOAD_REFERENCE(2);

    private final byte type;

    EncodeType(int type) {
      this.type = (byte) type;
    }

    byte getType() {
      return type;
    }
  }

  private final LevelDBPartitionManager partitionManager;

  LevelDBMessageTable(LevelDBPartitionManager partitionManager) {
    this.partitionManager = partitionManager;
  }

  @Override
  protected CloseableIterator<RawMessageTableEntry> scan(ScanRequest scanRequest) throws IOException {
    Collection<LevelDBPartition> partitions = partitionManager.getPartitions(scanRequest.getStartTime());
    if (partitions.isEmpty()) {
      return CloseableIterator.empty();
    }
    RawMessageTableEntry tableEntry = new RawMessageTableEntry();
    TopicMetadata topicMetadata = scanRequest.getTopicMetadata();
    byte[] topic = MessagingUtils.toDataKeyPrefix(topicMetadata.getTopicId(), topicMetadata.getGeneration());
    MessageTableKey messageTableKey = MessageTableKey.fromTopic(topic);
    BiFunction<byte[], byte[], RawMessageTableEntry> decodeFunction = (key, value) -> {
      Map<String, byte[]> columns = decodeValue(value);
      messageTableKey.setFromRowKey(key);
      return tableEntry.set(messageTableKey, columns.get(TX_COL), columns.get(PAYLOAD_COL));
    };

    return new PartitionedDBScanIterator<>(partitions.iterator(), scanRequest.getStartRow(), scanRequest.getStopRow(),
                                           decodeFunction);
  }

  @Override
  protected void persist(Iterator<RawMessageTableEntry> entries) throws IOException {
    // entries are sorted by publish time. accumulate all entries for a partition into a batch and
    // write the batch when the next entry is outside of the current partition
    LevelDBPartition partition = null;
    WriteBatch writeBatch = null;
    while (entries.hasNext()) {
      RawMessageTableEntry entry = entries.next();
      byte[] rowKey = entry.getKey().getRowKey();
      long publishTime = entry.getKey().getPublishTimestamp();

      // check if this entry belongs in a different partition. If so, write the current batch.
      if (partition == null || publishTime < partition.getStartTime() || publishTime >= partition.getEndTime()) {
        if (partition != null) {
          try {
            partition.getLevelDB().write(writeBatch, WRITE_OPTIONS);
          } finally {
            writeBatch.close();
          }
        }
        partition = partitionManager.getOrCreatePartition(publishTime);
        writeBatch = partition.getLevelDB().createWriteBatch();
      }

      // LevelDB doesn't make copies, and since we reuse RawMessageTableEntry object, we need to create copies.
      writeBatch.put(Arrays.copyOf(rowKey, rowKey.length), encodeValue(entry.getTxPtr(), entry.getPayload()));
    }

    if (partition != null) {
      try {
        partition.getLevelDB().write(writeBatch, WRITE_OPTIONS);
      } finally {
        writeBatch.close();
      }
    }
  }

  @Override
  protected void rollback(RollbackRequest rollbackRequest) throws IOException {
    Collection<LevelDBPartition> partitions = partitionManager.getPartitions(rollbackRequest.getStartTime(),
                                                                             rollbackRequest.getStopTime());

    for (LevelDBPartition partition : partitions) {
      DB levelDB = partition.getLevelDB();
      WriteBatch writeBatch = partition.getLevelDB().createWriteBatch();

      try (CloseableIterator<Map.Entry<byte[], byte[]>> rowIterator =
             new DBScanIterator(levelDB, rollbackRequest.getStartRow(), rollbackRequest.getStopRow())) {
        while (rowIterator.hasNext()) {
          Map.Entry<byte[], byte[]> rowValue = rowIterator.next();
          byte[] value = rowValue.getValue();
          Map<String, byte[]> columns = decodeValue(value);
          writeBatch.put(rowValue.getKey(), encodeValue(rollbackRequest.getTxWritePointer(), columns.get(PAYLOAD_COL)));
        }
      }

      try {
        levelDB.write(writeBatch, WRITE_OPTIONS);
      } catch (DBException ex) {
        throw new IOException(ex);
      }
    }
  }

  @Override
  public void close() {
    // This method has to be an no-op instead of closing the underlying LevelDB object
    // This is because a given LevelDB object instance is shared within the same JVM
  }

  // Encoding:
  // If the returned byte array starts with 0, then it is a non-tx message and all the subsequent bytes are payload
  // If the returned byte array starts with 1, then next 8 bytes correspond to txWritePtr and rest are payload bytes
  private byte[] encodeValue(@Nullable byte[] txWritePtr, @Nullable byte[] payload) {
    // Non-transactional
    if (txWritePtr == null) {
      // For non-tx message, payload cannot be null
      Preconditions.checkArgument(payload != null, "Payload cannot be null for non-transactional message");
      byte[] result = new byte[1 + payload.length];
      result[0] = EncodeType.NON_TRANSACTIONAL.getType();
      Bytes.putBytes(result, 1, payload, 0, payload.length);
      return result;
    }

    // Transactional
    if (payload != null) {
      byte[] result = new byte[1 + Bytes.SIZEOF_LONG + payload.length];
      result[0] = EncodeType.TRANSACTIONAL.getType();
      Bytes.putBytes(result, 1, txWritePtr, 0, txWritePtr.length);
      Bytes.putBytes(result, 1 + Bytes.SIZEOF_LONG, payload, 0, payload.length);
      return result;
    }

    // Transactional but without payload, hence it's a payload table reference
    byte[] result = new byte[1 + Bytes.SIZEOF_LONG];
    result[0] = EncodeType.PAYLOAD_REFERENCE.getType();
    Bytes.putBytes(result, 1, txWritePtr, 0, txWritePtr.length);
    return result;
  }

  private Map<String, byte[]> decodeValue(byte[] value) {
    Map<String, byte[]> data = new HashMap<>();

    if (value[0] == EncodeType.NON_TRANSACTIONAL.getType()) {
      data.put(PAYLOAD_COL, Arrays.copyOfRange(value, 1, value.length));
    } else {
      data.put(TX_COL, Arrays.copyOfRange(value, 1, 1 + Bytes.SIZEOF_LONG));

      // Only transactional type has payload, otherwise payload should be null.
      if (value[0] == EncodeType.TRANSACTIONAL.getType()) {
        data.put(PAYLOAD_COL, Arrays.copyOfRange(value, 1 + Bytes.SIZEOF_LONG, value.length));
      }
    }
    return data;
  }

}
