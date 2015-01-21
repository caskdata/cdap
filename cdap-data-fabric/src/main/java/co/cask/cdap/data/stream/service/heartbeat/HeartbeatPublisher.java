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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

/**
 * Publish {@link StreamWriterHeartbeat}s.
 */
public interface HeartbeatPublisher extends Service {

  /**
   * Publish one heartbeat.
   *
   * @param streamName Stream name on behalf of which to publish a heartbeat
   * @param heartbeat heartbeat to publish
   * @return a {@link ListenableFuture} describing the state of publishing. The {@link ListenableFuture#get} method
   * will return the published heartbeat.
   */
  ListenableFuture<StreamWriterHeartbeat> sendHeartbeat(String streamName, StreamWriterHeartbeat heartbeat);
}
