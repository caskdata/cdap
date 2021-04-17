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

import io.cdap.cdap.api.dataset.lib.cube.TimeValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a single time series defined by a metric name and set of tag values.
 */
public final class MetricTimeSeries {
  private final String metricName;
  private final Map<String, String> tagValues;
  private final List<TimeValue> timeValues;

  public MetricTimeSeries(String metricName, Map<String, String> tagValues, List<TimeValue> timeValues) {
    this.metricName = metricName;
    this.tagValues = Collections.unmodifiableMap(new HashMap<>(tagValues));
    this.timeValues = Collections.unmodifiableList(new ArrayList<>(timeValues));
  }

  public String getMetricName() {
    return metricName;
  }

  public Map<String, String> getTagValues() {
    return tagValues;
  }

  public List<TimeValue> getTimeValues() {
    return timeValues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MetricTimeSeries that = (MetricTimeSeries) o;

    return Objects.equals(metricName, that.metricName) &&
      Objects.equals(tagValues, that.tagValues) &&
      Objects.equals(timeValues, that.timeValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricName, tagValues, timeValues);
  }

  @Override
  public String toString() {
    return "MetricTimeSeries{" +
      "metricName='" + metricName + '\'' +
      ", tagValues=" + tagValues +
      ", timeValues=" + timeValues +
      '}';
  }
}
