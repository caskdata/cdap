/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime;

import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.util.Map;

/**
 *
 */
public final class EmittedDatum {

  private final QueueProducer queueProducer;
  private final URI queueName;
  private final byte[] data;
  private final Map<String, String> header;

  public EmittedDatum(QueueProducer queueProducer, URI queueName, byte[] data, Map<String, Object> partitions) {
    this.queueProducer = queueProducer;
    this.queueName = queueName;
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for(Map.Entry<String, Object> entry : partitions.entrySet()) {
      builder.put(entry.getKey(), String.valueOf(entry.getValue().hashCode()));
    }
    this.data = data;
    this.header = builder.build();
  }


  public QueueEnqueue asEnqueue() {
    //    return new QueueEnqueue(queueProducer,
    //                            queueName.toASCIIString().getBytes(Charsets.US_ASCII),
    //                            header, data);
    return null;
  }
}
