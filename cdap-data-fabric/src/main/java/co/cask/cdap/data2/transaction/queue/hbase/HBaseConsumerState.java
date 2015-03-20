/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.data2.transaction.queue.hbase;

/**
 * Represents state of queue consumer.
 */
public final class HBaseConsumerState {

  private final long groupId;
  private final int instanceId;
  private final byte[] startRow;

  HBaseConsumerState(byte[] startRow, long groupId, int instanceId) {
    this.startRow = startRow;
    this.groupId = groupId;
    this.instanceId = instanceId;
  }

  public byte[] getStartRow() {
    return startRow;
  }

  public long getGroupId() {
    return groupId;
  }

  public int getInstanceId() {
    return instanceId;
  }
}
