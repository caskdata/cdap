/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.engine.sql.request;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineOutput;

import java.io.Serializable;

/**
 * A request to perform write operation
 */
@Beta
public class SQLWriteRequest implements Serializable {
  private final String datasetName;
  private final SQLEngineOutput output;

  public SQLWriteRequest(String datasetName, SQLEngineOutput output) {
    this.datasetName = datasetName;
    this.output = output;
  }

  /**
   * Get the name of the dataset that should be written to the sink.
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   * Describes the sink destination
   */
  public SQLEngineOutput getOutput() {
    return output;
  }
}
