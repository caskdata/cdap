package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationBase;
import com.continuuity.api.data.ReadOperation;
import com.continuuity.common.utils.Bytes;
import com.google.common.base.Objects;

/**
 * Marks and retrieves the next entry in the queue for the specified consumer,
 * starting from the head of the queue. Entry selected depends on the mode of
 * dequeue execution.
 */
public class QueueDequeue implements ReadOperation {

  /** Unique id for the operation */
  private final long id = OperationBase.getId();
  private final byte [] queueName;
  private final QueueConsumer consumer;
  private final QueueConfig config;

  public QueueDequeue(final byte [] queueName, final QueueConsumer consumer,
      final QueueConfig config) {
    this.queueName = queueName;
    this.consumer = consumer;
    this.config = config;
  }

  public byte[] getKey() {
    return this.queueName;
  }

  public QueueConsumer getConsumer() {
    return this.consumer;
  }

  public QueueConfig getConfig() {
    return this.config;
  }

  public String toString() {
    return Objects.toStringHelper(this)
        .add("queueName", Bytes.toString(this.queueName))
        .add("consumer", Objects.toStringHelper(this.consumer))
        .add("config", this.config.toString())
        .toString();
  }

  @Override
  public long getId() {
    return id;
  }
}
