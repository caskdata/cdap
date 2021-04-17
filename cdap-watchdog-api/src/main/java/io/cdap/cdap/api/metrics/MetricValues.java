/*
 * Copyright 2014 Cask Data, Inc.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Carries the "raw" emitted metric data points: context, timestamp, collection of
 * {@link io.cdap.cdap.api.metrics.MetricValue}
 */
public class MetricValues {
  private final Map<String, String> tags;
  private final Collection<MetricValue> metrics;

  /**
   * Timestamp in seconds.
   */
  private final long timestamp;

  public MetricValues(Map<String, String> tags, String name, long timestamp, long value, MetricType type) {
    this(tags, timestamp, Collections.singleton(new MetricValue(name, type, value)));
  }

  public MetricValues(Map<String, String> tags, long timestamp, Collection<MetricValue> metrics) {
    this.tags = Collections.unmodifiableMap(new HashMap<>(tags));
    this.timestamp = timestamp;
    this.metrics = Collections.unmodifiableCollection(new ArrayList<>(metrics));
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public Collection<MetricValue> getMetrics() {
    return metrics;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "MetricValues{" +
      "tags=" + tags +
      ", metrics=" + metrics +
      ", timestamp=" + timestamp +
      '}';
  }
}
