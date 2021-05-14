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

/**
 * Request to rollback transactionally published messages in a given key range.
 */
public class RollbackRequest {
  private final byte[] startRow;
  private final byte[] stopRow;
  private final byte[] txWritePointer;
  private final long startTime;
  private final long stopTime;

  public RollbackRequest(byte[] startRow, byte[] stopRow, byte[] txWritePointer, long startTime, long stopTime) {
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.txWritePointer = txWritePointer;
    this.startTime = startTime;
    this.stopTime = stopTime;
  }

  public byte[] getStartRow() {
    return startRow;
  }

  public byte[] getStopRow() {
    return stopRow;
  }

  public byte[] getTxWritePointer() {
    return txWritePointer;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getStopTime() {
    return stopTime;
  }
}
