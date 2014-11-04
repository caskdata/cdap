/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.proto;

import com.google.common.base.Objects;

import java.util.List;

/**
 * Represents query result.
 */
public class QueryResult {

  /**
   * Type of an column in a query result row.
   */
  public enum ResultType {
    BOOLEAN,
    BYTE,
    SHORT,
    INT,
    LONG,
    DOUBLE,
    STRING
  }

  private final List<ResultObject> columns;

  public QueryResult(List<ResultObject> columns) {
    this.columns = columns;
  }

  public List<ResultObject> getColumns() {
    return columns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    QueryResult that = (QueryResult) o;

    return Objects.equal(this.columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(columns);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("columns", columns)
      .toString();
  }

  /**
   * Represents one object of a query result.
   */
  public static class ResultObject {
    Object object;
    ResultType type;

    public ResultObject(Object object, ResultType type) {
      this.object = object;
      this.type = type;
    }

    public Object getObject() {
      return object;
    }

    public ResultType getType() {
      return type;
    }

    @Override
    public String toString() {
      return object.toString();
    }
  }
}
