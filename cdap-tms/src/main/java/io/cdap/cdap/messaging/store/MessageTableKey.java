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

import io.cdap.cdap.api.common.Bytes;

/**
 * Row key used for an entry in a MessageTable.
 */
public class MessageTableKey {
  private final byte[] rowKey;
  private final int topicLength;
  private long publishTimestamp;
  private short sequenceId;

  public static MessageTableKey fromRowKey(byte[] rowKey) {
    int topicLength = rowKey.length - Bytes.SIZEOF_LONG - Bytes.SIZEOF_SHORT;
    short sequenceId = Bytes.toShort(rowKey, topicLength + Bytes.SIZEOF_LONG, Bytes.SIZEOF_SHORT);
    long publishTimestamp = Bytes.toLong(rowKey, topicLength, Bytes.SIZEOF_LONG);
    byte[] topic = Bytes.head(rowKey, topicLength);
    MessageTableKey messageTableKey = new MessageTableKey(topic);
    messageTableKey.set(publishTimestamp, sequenceId);
    return messageTableKey;
  }

  public static MessageTableKey fromTopic(byte[] topic) {
    return new MessageTableKey(topic);
  }

  private MessageTableKey(byte[] topic) {
    this.topicLength = topic.length;
    this.rowKey = new byte[topicLength + Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT];
    Bytes.putBytes(rowKey, 0, topic, 0, topic.length);
  }

  /**
   * Set the publish timestamp and sequence id to the values set in the given row key.
   * This method should only be used when the topic is the same, otherwise a new instance of the
   * MessageTableKey should be created.
   */
  public void setFromRowKey(byte[] rowKey) {
    long publishTimestamp = Bytes.toLong(rowKey, topicLength, Bytes.SIZEOF_LONG);
    short sequenceId = Bytes.toShort(rowKey, topicLength + Bytes.SIZEOF_LONG, Bytes.SIZEOF_SHORT);
    set(publishTimestamp, sequenceId);
  }

  public void set(long publishTimestamp, short sequenceId) {
    this.publishTimestamp = publishTimestamp;
    this.sequenceId = sequenceId;
    Bytes.putLong(rowKey, topicLength, publishTimestamp);
    Bytes.putShort(rowKey, topicLength + Bytes.SIZEOF_LONG, sequenceId);
  }

  public byte[] getRowKey() {
    return rowKey;
  }

  public long getPublishTimestamp() {
    return publishTimestamp;
  }

  public short getSequenceId() {
    return sequenceId;
  }
}
