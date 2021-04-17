/*
 * Copyright 2015 Cask Data, Inc.
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
package io.cdap.cdap.api.metrics;

/**
 * Carries the "raw" emitted metric data point: metric name, type, and value
 */
public class MetricValue {

  private String name;
  private MetricType type;
  private long value;

  public MetricValue (String name, MetricType type, long value) {
    this.name = name;
    this.type = type;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public MetricType getType() {
    return type;
  }

  public long getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "MetricValue{" +
      "name='" + name + '\'' +
      ", type=" + type +
      ", value=" + value +
      '}';
  }
}
