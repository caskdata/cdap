/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.service.heartbeat;

import com.google.common.base.Objects;

/**
 * Heartbeat sent by a Stream writer containing the total size of its files, in bytes..
 */
public class StreamWriterHeartbeat {

  /**
   * Type of heartbeat, describing at what moment it is sent by a Stream writer.
   */
  public enum Type {
    /**
     * Heartbeat sent during Stream writer initialization.
     */
    INIT,

    /**
     * Heartbeat sent during regular Stream writer life.
     */
    REGULAR
  }

  private final long timestamp;
  private final long absoluteDataSize;
  private final int writerID;
  private final Type type;

  public StreamWriterHeartbeat(long timestamp, long absoluteDataSize, int writerID, Type type) {
    this.timestamp = timestamp;
    this.absoluteDataSize = absoluteDataSize;
    this.writerID = writerID;
    this.type = type;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public int getWriterID() {
    return writerID;
  }

  public long getAbsoluteDataSize() {
    return absoluteDataSize;
  }

  public Type getType() {
    return type;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(StreamWriterHeartbeat.class)
      .add("timestamp", timestamp)
      .add("absoluteDataSize", absoluteDataSize)
      .add("writerID", writerID)
      .add("type", type)
      .toString();
  }
}
