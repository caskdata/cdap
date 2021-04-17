/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A no-op implementation of {@link MetricsContext}.
 */
public final class NoopMetricsContext implements MetricsContext {

  private final Map<String, String> tags;

  public NoopMetricsContext() {
    this(Collections.emptyMap());
  }

  public NoopMetricsContext(Map<String, String> tags) {
    this.tags = Collections.unmodifiableMap(new HashMap<>(tags));
  }

  @Override
  public MetricsContext childContext(Map<String, String> tags) {
    Map<String, String> context = new HashMap<>(this.tags);
    context.putAll(tags);
    return new NoopMetricsContext(context);
  }

  @Override
  public MetricsContext childContext(String tagName, String tagValue) {
    return childContext(Collections.singletonMap(tagName, tagValue));
  }

  @Override
  public Map<String, String> getTags() {
    return tags;
  }

  @Override
  public void increment(String metricName, long value) {
    // no-op
  }

  @Override
  public void gauge(String metricName, long value) {
    // no-op
  }
}
