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

package co.cask.cdap.etl.transform;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.Lookup;
import org.slf4j.Logger;

/**
 * Context passed to {@link co.cask.cdap.etl.transform.ScriptTransform} and
 * {@link co.cask.cdap.etl.transform.ScriptFilterTransform} script's.
 */
public class ScriptContext {
  private final Logger logger;
  private final Metrics metrics;
  private final Lookup lookup;

  public ScriptContext(Logger logger, Metrics metrics, Lookup lookup) {
    this.logger = logger;
    this.metrics = metrics;
    this.lookup = lookup;
  }

  public Logger getLogger() {
    return logger;
  }

  public Metrics getMetrics() {
    return metrics;
  }

  public Lookup getLookup() {
    return lookup;
  }
}
