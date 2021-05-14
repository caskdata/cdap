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

package io.cdap.cdap.messaging.store;

import com.google.common.collect.AbstractIterator;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.messaging.MessagingUtils;
import io.cdap.cdap.messaging.RollbackDetail;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.id.TopicId;
import org.apache.tephra.Transaction;

import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * Contains common logic for implementation of {@link MessageTable}.
 */
public abstract class AbstractMessageTable implements MessageTable {

  /**
   * Store the {@link RawMessageTableEntry}s persistently. Entries are sorted by publish time.
   *
   * @param entries {@link Iterator} of {@link RawMessageTableEntry}s
   * @throws IOException thrown if there was an error while storing the entries
   */
  protected abstract void persist(Iterator<RawMessageTableEntry> entries) throws IOException;

  /**
   * Rollback the transactionally published messages in the Table in the given key range.
   *
   * @param rollbackRequest information about the keys to roll back
   * @throws IOException thrown if there was an error while trying to delete the entries
   */
  protected abstract void rollback(RollbackRequest rollbackRequest) throws IOException;

  /**
   * Read the {@link RawMessageTableEntry}s given a key range.
   *
   * @param scanRequest information about the scan to perform
   * @return {@link CloseableIterator} of {@link RawMessageTableEntry}s
   * @throws IOException throw if there was an error while trying to read the entries from the table
   */
  protected abstract CloseableIterator<RawMessageTableEntry> scan(ScanRequest scanRequest) throws IOException;

  @Override
  public CloseableIterator<Entry> fetch(TopicMetadata metadata, long startTime, int limit,
                                        @Nullable Transaction transaction) throws IOException {
    byte[] topic = MessagingUtils.toDataKeyPrefix(metadata.getTopicId(), metadata.getGeneration());
    byte[] startRow = new byte[topic.length + Bytes.SIZEOF_LONG];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, startTime);
    byte[] stopRow = Bytes.stopKeyForPrefix(topic);
    ScanRequest scanRequest = new ScanRequest(metadata, startRow, stopRow, startTime);
    CloseableIterator<RawMessageTableEntry> scanner = scan(scanRequest);
    return new FetchIterator(scanner, limit, null, transaction);
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicMetadata metadata, MessageId messageId, boolean inclusive,
                                        final int limit, @Nullable final Transaction transaction) throws IOException {
    byte[] topic = MessagingUtils.toDataKeyPrefix(metadata.getTopicId(), metadata.getGeneration());
    byte[] startRow = new byte[topic.length + Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, messageId.getPublishTimestamp());
    Bytes.putShort(startRow, topic.length + Bytes.SIZEOF_LONG, messageId.getSequenceId());
    byte[] stopRow = Bytes.stopKeyForPrefix(topic);
    ScanRequest scanRequest = new ScanRequest(metadata, startRow, stopRow, messageId.getPublishTimestamp());
    CloseableIterator<RawMessageTableEntry> scanner = scan(scanRequest);
    return new FetchIterator(scanner, limit, inclusive ? null : startRow, transaction);
  }

  @Override
  public void store(Iterator<? extends Entry> entries) throws IOException {
    persist(new StoreIterator(entries));
  }

  @Override
  public void rollback(TopicMetadata metadata, RollbackDetail rollbackDetail) throws IOException {
    //long startTimestamp, short startSequenceId,
    //long endTimestamp, short endSequenceId
    byte[] topic = MessagingUtils.toDataKeyPrefix(metadata.getTopicId(), metadata.getGeneration());
    byte[] startRow = new byte[topic.length + Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, rollbackDetail.getStartTimestamp());
    Bytes.putShort(startRow, topic.length + Bytes.SIZEOF_LONG, (short) rollbackDetail.getStartSequenceId());

    byte[] stopRow = new byte[topic.length + Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT];
    Bytes.putBytes(stopRow, 0, topic, 0, topic.length);
    Bytes.putLong(stopRow, topic.length, rollbackDetail.getEndTimestamp());
    Bytes.putShort(stopRow, topic.length + Bytes.SIZEOF_LONG, (short) rollbackDetail.getEndSequenceId());

    byte[] txWritePointer = Bytes.toBytes(-1 * rollbackDetail.getTransactionWritePointer());
    RollbackRequest rollbackRequest = new RollbackRequest(startRow, Bytes.stopKeyForPrefix(stopRow),
                                                          txWritePointer,
                                                          rollbackDetail.getStartTimestamp(),
                                                          rollbackDetail.getEndTimestamp());
    rollback(rollbackRequest);
  }

  /**
   * An {@link Iterator} for fetching {@link Entry} from the the message table.
   */
  private static class FetchIterator extends AbstractCloseableIterator<Entry> {
    private final CloseableIterator<RawMessageTableEntry> scanner;
    private final TransactionMessageFilter filter;
    private byte[] skipStartRow;
    private boolean closed = false;
    private int maxLimit;

    FetchIterator(CloseableIterator<RawMessageTableEntry> scanner, int limit, @Nullable byte[] skipStartRow,
                  @Nullable Transaction transaction) {
      this.scanner = scanner;
      this.filter =  transaction == null ? null : new TransactionMessageFilter(transaction);
      this.skipStartRow = skipStartRow;
      this.maxLimit = limit;
    }

    @Override
    protected Entry computeNext() {
      if (closed || (maxLimit <= 0)) {
        return endOfData();
      }

      while (scanner.hasNext()) {
        RawMessageTableEntry tableEntry = scanner.next();

        // See if we need to skip the first row returned by the scanner
        if (skipStartRow != null) {
          byte[] row = skipStartRow;
          // After first row, we don't need to match anymore
          skipStartRow = null;
           if (Bytes.equals(row, tableEntry.getKey().getRowKey())) {
             continue;
           }
        }
        MessageFilter.Result status = accept(tableEntry.getTxPtr());
        if (status == MessageFilter.Result.ACCEPT) {
          maxLimit--;
          return new ImmutableMessageTableEntry(tableEntry.getKey().getRowKey(),
                                                tableEntry.getPayload(), tableEntry.getTxPtr());
        }

        if (status == MessageFilter.Result.HOLD) {
          break;
        }
      }
      return endOfData();
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

    private MessageFilter.Result accept(@Nullable byte[] txPtr) {
      // No transaction info available, so accept this message (it must have been published non-transactionally)
      if (filter == null || txPtr == null) {
        return MessageFilter.Result.ACCEPT;
      }

      return filter.filter(Bytes.toLong(txPtr));
    }
  }

  /**
   * A {@link Iterator} for iterating over {@link RawMessageTableEntry} based on a given
   * iterator of {@link Entry}.
   */
  private static class StoreIterator extends AbstractIterator<RawMessageTableEntry> {

    private final Iterator<? extends Entry> entries;
    private final RawMessageTableEntry tableEntry;
    private TopicId topicId;
    private int generation;
    private byte[] topic;
    private MessageTableKey key;

    private StoreIterator(Iterator<? extends Entry> entries) {
      this.entries = entries;
      this.tableEntry = new RawMessageTableEntry();
    }

    @Override
    protected RawMessageTableEntry computeNext() {
      if (!entries.hasNext()) {
        return endOfData();
      }

      Entry entry = entries.next();
      // Create new byte arrays only when the topicId is different. Else, reuse the byte arrays.
      if (topicId == null || (!topicId.equals(entry.getTopicId())) || (generation != entry.getGeneration())) {
        topicId = entry.getTopicId();
        generation = entry.getGeneration();
        topic = MessagingUtils.toDataKeyPrefix(topicId, entry.getGeneration());
        key = MessageTableKey.fromTopic(topic);
      }

      key.set(entry.getPublishTimestamp(), entry.getSequenceId());

      byte[] txPtr = null;
      if (entry.isTransactional()) {
        txPtr = Bytes.toBytes(entry.getTransactionWritePointer());
      }
      return tableEntry.set(key, txPtr, entry.getPayload());
    }
  }
}
