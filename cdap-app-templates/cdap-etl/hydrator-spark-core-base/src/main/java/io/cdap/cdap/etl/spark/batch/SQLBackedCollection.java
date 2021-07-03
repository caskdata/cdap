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

package io.cdap.cdap.etl.spark.batch;

import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkCollection;
import org.apache.spark.api.java.function.PairFlatMapFunction;

/**
 * Interface to denote collections where the underlying records are stored in a SQL engine.
 *
 * @param <T> type of elements in the spark collection
 */
public interface SQLBackedCollection<T> extends SparkCollection<T> {
  boolean tryStoreDirect(StageSpec stageSpec);
}
