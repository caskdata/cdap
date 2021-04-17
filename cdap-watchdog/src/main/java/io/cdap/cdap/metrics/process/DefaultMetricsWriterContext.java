/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.metrics.process;

import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.MetricsWriterContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import java.util.Collections;
import java.util.Map;

/**
 * Default implementation of {@link MetricsWriterContext}.
 */
public class DefaultMetricsWriterContext implements MetricsWriterContext {

  private final MetricsContext metricsContext;
  private final Map<String, String> properties;

  DefaultMetricsWriterContext(MetricsContext metricsContext, CConfiguration cConf, String metricsWriterId) {
    this.metricsContext = metricsContext;
    String prefix = String.format("%s%s.", Constants.Metrics.METRICS_WRITER_PREFIX, metricsWriterId);
    this.properties = Collections.unmodifiableMap(cConf.getPropsWithPrefix(prefix));
  }

  public MetricsContext getMetricsContext() {
    return metricsContext;
  }

  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(properties);
  }
}
