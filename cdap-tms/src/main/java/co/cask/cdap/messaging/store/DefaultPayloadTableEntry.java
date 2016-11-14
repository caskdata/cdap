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

package co.cask.cdap.messaging.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.proto.id.TopicId;

/**
* Payload Entry.
*/
public final class DefaultPayloadTableEntry implements PayloadTable.Entry {
  private final TopicId topicId;
  private final long transactionWriterPointer;
  private final long publishTimestamp;
  private final short sequenceId;
  private final byte[] payload;

  public DefaultPayloadTableEntry(byte[] row, byte[] payload) {
    this.topicId = MessagingUtils.toTopicId(row, 0, row.length - Short.BYTES - (2 * Long.BYTES));
    this.transactionWriterPointer = Bytes.toLong(row, row.length - Short.BYTES - (2 * Long.BYTES));
    this.publishTimestamp = Bytes.toLong(row, row.length - Short.BYTES - Long.BYTES);
    this.sequenceId = Bytes.toShort(row, row.length - Short.BYTES);
    this.payload = payload;
  }

  @Override
  public TopicId getTopicId() {
    return topicId;
  }

  @Override
  public byte[] getPayload() {
    return payload;
  }

  @Override
  public long getTransactionWritePointer() {
    return transactionWriterPointer;
  }

  @Override
  public long getPayloadWriteTimestamp() {
    return publishTimestamp;
  }

  @Override
  public short getPayloadSequenceId() {
    return sequenceId;
  }
}
