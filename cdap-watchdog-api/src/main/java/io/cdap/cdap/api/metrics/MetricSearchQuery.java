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

package io.cdap.cdap.api.metrics;

import java.util.List;

/**
 * Defines a query to perform Exploration and Search on {@link MetricStore} data.
 * Given a list of {@link TagValue}s this explore query can be used
 * to find next set of tags available or the measureNames belonging to this tag list.
 */
public class MetricSearchQuery {
  private final long startTs;
  private final long endTs;
  private final int resolution;
  private final int limit;
  // todo: use NavigableMap to inform that there are no same tag names?
  private final List<TagValue> tagValues;

  public MetricSearchQuery(long startTs, long endTs, int resolution, int limit, List<TagValue> tagValues) {
    this.startTs = startTs;
    this.endTs = endTs;
    this.resolution = resolution;
    this.limit = limit;
    this.tagValues = tagValues;
  }

  public MetricSearchQuery(long startTs, long endTs, int limit, List<TagValue> tagValues) {
    // we use MAX-RESOLUTION for search as they contain all the metric-names
    // that have been emitted and these metrics don't expire.
    this(startTs, endTs, Integer.MAX_VALUE, limit, tagValues);
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

  public int getLimit() {
    return limit;
  }

  public List<TagValue> getTagValues() {
    return tagValues;
  }

  @Override
  public String toString() {
    return "MetricSearchQuery{" +
      "startTs=" + startTs +
      ", endTs=" + endTs +
      ", resolution=" + resolution +
      ", limit=" + limit +
      ", tagValues=" + tagValues +
      '}';
  }
}
