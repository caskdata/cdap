/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.logging.appender.remote;


import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.common.internal.remote.RemoteClient;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.logging.appender.AbstractLogPublisher;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.appender.kafka.LogPartitionType;
import co.cask.cdap.logging.serialize.LogSchema;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.hash.Hashing;
import com.google.common.net.HttpHeaders;
import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Remote log appender to push logs to log saver.
 */
public class RemoteLogAppender extends LogAppender {
  private static final String APPENDER_NAME = "RemoteLogAppender";
  private final RemoteClient remoteClient;
  private final RemoteLogPublisher publisher;

  @Inject
  RemoteLogAppender(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient) {
    setName(APPENDER_NAME);
    int queueSize = cConf.getInt(Constants.Logging.APPENDER_QUEUE_SIZE, 512);
    this.publisher = new RemoteLogPublisher(cConf, queueSize);
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.LOG_BUFFER_SERVICE,
                                         new DefaultHttpRequestConfig(), "/v1/");
  }

  @Override
  public void start() {
    publisher.startAndWait();
    addInfo("Successfully started " + APPENDER_NAME);
    super.start();
  }

  @Override
  public void stop() {
    publisher.stopAndWait();
    addInfo("Successfully stopped " + APPENDER_NAME);
    super.stop();
  }

  @Override
  protected void appendEvent(LogMessage logMessage) {
    logMessage.prepareForDeferredProcessing();
    logMessage.getCallerData();

    try {
      publisher.addMessage(logMessage);
    } catch (InterruptedException e) {
      addInfo("Interrupted when adding log message to queue: " + logMessage.getFormattedMessage());
    }
  }

  /**
   * Publisher service to publish logs to log saver.
   */
  private final class RemoteLogPublisher extends AbstractLogPublisher<Map.Entry<Integer, byte[]>> {

    private final int numPartitions;
    private final LoggingEventSerializer loggingEventSerializer;
    private final LogPartitionType logPartitionType;

    private RemoteLogPublisher(CConfiguration cConf, int queueSize) {
      super(queueSize, RetryStrategies.fromConfiguration(cConf, "system.log.process."));
      this.numPartitions = cConf.getInt(Constants.Logging.NUM_PARTITIONS);
      this.loggingEventSerializer = new LoggingEventSerializer();
      this.logPartitionType =
        LogPartitionType.valueOf(cConf.get(Constants.Logging.LOG_PUBLISH_PARTITION_KEY).toUpperCase());
    }

    @Override
    protected Map.Entry<Integer, byte[]> createMessage(LogMessage logMessage) {
      String partitionKey = logPartitionType.getPartitionKey(logMessage.getLoggingContext());
      int partition = partition(partitionKey, numPartitions);
      return new AbstractMap.SimpleEntry<>(partition, loggingEventSerializer.toBytes(logMessage));
    }

    @Override
    protected void publish(List<Map.Entry<Integer, byte[]>> logMessages) throws Exception {
      // Group the log messages by partition and then publish all messages to their respective partitions
      Map<Integer, List<byte[]>> partitionedMessages = new HashMap<>();
      for (Map.Entry<Integer, byte[]> logMessage : logMessages) {
        List<byte[]> messages = partitionedMessages.computeIfAbsent(logMessage.getKey(), k -> new ArrayList<>());
        messages.add(logMessage.getValue());
      }

      for (Map.Entry<Integer, List<byte[]>> partition : partitionedMessages.entrySet()) {
        HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "process")
          .addHeader(HttpHeaders.CONTENT_TYPE, "avro/binary")
          .withBody(encodeEvents(partition.getKey(), partition.getValue())).build();
        HttpResponse response = remoteClient.execute(request);
        // if something went wrong, throw exception to retry
        if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
          throw new IOException(String.format("Could not append logs for partition %s", partition));
        }
      }
    }

    @Override
    protected void logError(String errorMessage, Exception exception) {
      // Log using the status manager
      addError(errorMessage, exception);
    }
  }

  private static int partition(Object key, int numPartitions) {
    return Math.abs(Hashing.md5().hashString(key.toString()).asInt()) % numPartitions;
  }

  private ByteBuffer encodeEvents(int partition, List<byte[]> events) throws IOException {
    Schema schema = LogSchema.LogBufferRequest.SCHEMA;
    GenericRecord record = new GenericData.Record(schema);
    record.put("partition", partition);
    record.put("events", events);

    ExposedByteArrayOutputStream os = new ExposedByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(os, null);

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    datumWriter.write(record, encoder);

    return os.toByteBuffer();
  }

  /**
   * A {@link ByteArrayOutputStream} that exposes the written raw buffer as ByteBuffer.
   */
  private static final class ExposedByteArrayOutputStream extends ByteArrayOutputStream {
    /**
     * Returns a {@link ByteBuffer} that represents the valid content in the buffer.
     */
    ByteBuffer toByteBuffer() {
      return ByteBuffer.wrap(buf, 0, count);
    }
  }
}
