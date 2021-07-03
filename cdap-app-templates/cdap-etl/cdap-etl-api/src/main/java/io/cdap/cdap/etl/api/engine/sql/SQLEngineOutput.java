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

package io.cdap.cdap.etl.api.engine.sql;

import io.cdap.cdap.api.data.batch.Output;

import java.io.Serializable;
import java.util.Map;

/**
 * Sinks that want to employ optimization by an SQL Engine may push SQLEngine-specific outputs in addition
 * to regular ones
 */
public class SQLEngineOutput extends Output {
  private final String sqlEngineClassName;
  private final Map<String, String> arguments;

  public SQLEngineOutput(String name, String sqlEngineClassName, Map<String, String> arguments) {
    super(name);
    this.sqlEngineClassName = sqlEngineClassName;
    this.arguments = arguments;
  }

  public String getSqlEngineClassName() {
    return sqlEngineClassName;
  }

  public Map<String, String> getArguments() {
    return arguments;
  }
}
