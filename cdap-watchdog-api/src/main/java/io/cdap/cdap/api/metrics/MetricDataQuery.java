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

import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.AggregationOption;
import io.cdap.cdap.api.dataset.lib.cube.Interpolator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Defines a query to perform on {@link MetricStore} data.
 * </p>
 * Though limited currently in functionality, you can map {@link MetricDataQuery} to the following statement:
 * <pre>
 * SELECT count('read.ops')                                     << metric name and aggregation function
 * FROM Cube
 * GROUP BY dataset,                                            << groupByTags
 * WHERE namespace='ns1' AND app='myApp' AND program='myFlow'   << sliceByTags
 *
 * </pre>
 */
public final class MetricDataQuery {

  /**
   * Start timestamp, in seconds.
   */
  private final long startTs;

  /**
   * End timestamp, in seconds.
   */
  private final long endTs;

  /**
   * Resolution in seconds.
   */
  private final int resolution;
  private final int limit;
  private final Map<String, AggregationFunction> metrics;
  private final Map<String, String> sliceByTagValues;
  private final List<String> groupByTags;
  private final AggregationOption aggregationOption;

  private final Interpolator interpolator;

  /**
   * @param startTs Start timestamp, in seconds.
   * @param endTs End timestamp, in seconds.
   * @param resolution Resolution in seconds.
   */
  public MetricDataQuery(long startTs, long endTs, int resolution,
                         String metricName, AggregationFunction func,
                         Map<String, String> sliceByTagValues, List<String> groupByTags) {
    this(startTs, endTs, resolution, Integer.MAX_VALUE, Collections.singletonMap(metricName, func), sliceByTagValues,
         groupByTags, null);
  }

  /**
   * @param startTs Start timestamp, in seconds.
   * @param endTs End timestamp, in seconds.
   * @param resolution Resolution in seconds.
   */
  public MetricDataQuery(long startTs, long endTs, int resolution,
                         Map<String, AggregationFunction> metrics,
                         Map<String, String> sliceByTagValues, List<String> groupByTags) {
    this(startTs, endTs, resolution, Integer.MAX_VALUE, metrics, sliceByTagValues, groupByTags, null);
  }

  /**
   * @param startTs Start timestamp, in seconds.
   * @param endTs End timestamp, in seconds.
   * @param resolution Resolution in seconds.
   */
  public MetricDataQuery(long startTs, long endTs, int resolution, int limit,
                         Map<String, AggregationFunction> metrics,
                         Map<String, String> sliceByTagValues, List<String> groupByTags,
                         @Nullable Interpolator interpolator) {
    this(startTs, endTs, resolution, limit, metrics, sliceByTagValues, groupByTags,
         AggregationOption.FALSE, interpolator);
  }

  public MetricDataQuery(long startTs, long endTs, int resolution, int limit,
                         Map<String, AggregationFunction> metrics,
                         Map<String, String> sliceByTagValues, List<String> groupByTags,
                         AggregationOption aggregationOption,
                         @Nullable Interpolator interpolator) {
    this.startTs = startTs;
    this.endTs = endTs;
    this.resolution = resolution;
    this.limit = limit;
    this.metrics = metrics;
    this.sliceByTagValues = sliceByTagValues;
    this.groupByTags = groupByTags;
    this.aggregationOption = aggregationOption;
    this.interpolator = interpolator;
  }

  public long getStartTs() {
    return startTs;
  }

  public long getEndTs() {
    return endTs;
  }

  public int getResolution() {
    return resolution;
  }

  public Map<String, AggregationFunction> getMetrics() {
    return metrics;
  }

  public Map<String, String> getSliceByTags() {
    return sliceByTagValues;
  }

  public List<String> getGroupByTags() {
    return groupByTags;
  }

  // todo: push down limit support to Cube
  public int getLimit() {
    return limit;
  }

  public AggregationOption getAggregationOption() {
    return aggregationOption;
  }

  public Interpolator getInterpolator() {
    return interpolator;
  }

  @Override
  public String toString() {
    return "MetricDataQuery{" +
      "startTs=" + startTs +
      ", endTs=" + endTs +
      ", resolution=" + resolution +
      ", limit=" + limit +
      ", metrics=" + metrics +
      ", sliceByTagValues=" + sliceByTagValues +
      ", groupByTags=" + groupByTags +
      ", aggregationOption=" + aggregationOption +
      ", interpolator=" + interpolator +
      '}';
  }
}
