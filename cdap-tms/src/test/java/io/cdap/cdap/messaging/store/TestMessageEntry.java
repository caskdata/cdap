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

package io.cdap.cdap.messaging.store;

import io.cdap.cdap.proto.id.TopicId;

import javax.annotation.Nullable;

// class for publishing messages
public class TestMessageEntry implements MessageTable.Entry {
  private final TopicId topicId;
  private final int generation;
  private final Long transactionWritePointer;
  private final byte[] payload;
  private final long publishTimestamp;
  private final short sequenceId;

  public TestMessageEntry(TopicId topicId, int generation, long publishTimestamp, int sequenceId,
                   @Nullable Long transactionWritePointer, @Nullable byte[] payload) {
    this.topicId = topicId;
    this.generation = generation;
    this.transactionWritePointer = transactionWritePointer;
    this.publishTimestamp = publishTimestamp;
    this.sequenceId = (short) sequenceId;
    this.payload = payload;
  }

  @Override
  public TopicId getTopicId() {
    return topicId;
  }

  @Override
  public int getGeneration() {
    return generation;
  }

  @Override
  public boolean isPayloadReference() {
    return payload == null;
  }

  @Override
  public boolean isTransactional() {
    return transactionWritePointer != null;
  }

  @Override
  public long getTransactionWritePointer() {
    return transactionWritePointer == null ? -1L : transactionWritePointer;
  }

  @Nullable
  @Override
  public byte[] getPayload() {
    return payload;
  }

  @Override
  public long getPublishTimestamp() {
    return publishTimestamp;
  }

  @Override
  public short getSequenceId() {
    return sequenceId;
  }
}
