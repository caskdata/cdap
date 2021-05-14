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

import javax.annotation.Nullable;

/**
 * Container class that contains raw bytes corresponding to an entry in the Message Table.
 */
public class RawMessageTableEntry {
  private MessageTableKey key;
  private byte[] txPtr;
  private byte[] payload;

  public RawMessageTableEntry set(MessageTableKey key, @Nullable byte[] txPtr, @Nullable byte[] payload) {
    this.key = key;
    this.txPtr = txPtr;
    this.payload = payload;
    return this;
  }

  public MessageTableKey getKey() {
    return key;
  }

  @Nullable
  public byte[] getTxPtr() {
    return txPtr;
  }

  @Nullable
  public byte[] getPayload() {
    return payload;
  }
}
