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

package co.cask.cdap.templates.etl.transforms;

import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Transform;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import javax.annotation.Nullable;

/**
 * Transform {@link GenericRecord} to {@link AvroKey<GenericRecord>}
 */
public class GenericRecordToAvroKeyTransform extends Transform<LongWritable, GenericRecord,
                                                               NullWritable, AvroKey<GenericRecord>> {
  @Override
  public void transform(@Nullable LongWritable inputKey, GenericRecord genericRecord,
                        Emitter<NullWritable, AvroKey<GenericRecord>> emitter) throws Exception {

    emitter.emit(null, new AvroKey<GenericRecord>(genericRecord));
  }
}
