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

import io.cdap.cdap.messaging.TopicMetadata;

/**
 * Request to scan across a key range in a MessageTable.
 */
public class ScanRequest {
  private final TopicMetadata topicMetadata;
  private final byte[] startRow;
  private final byte[] stopRow;
  private final long startTime;

  public ScanRequest(TopicMetadata topicMetadata, byte[] startRow, byte[] stopRow, long startTime) {
    this.topicMetadata = topicMetadata;
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.startTime = startTime;
  }

  public TopicMetadata getTopicMetadata() {
    return topicMetadata;
  }

  public byte[] getStartRow() {
    return startRow;
  }

  public byte[] getStopRow() {
    return stopRow;
  }

  public long getStartTime() {
    return startTime;
  }
}
