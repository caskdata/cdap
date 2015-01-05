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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

import java.io.IOException;
import java.util.Map;

/**
 * {@link DatasetDefinition} for {@link StateStoreTable}.
 */
public class StateStoreTableDefinition extends AbstractDatasetDefinition<StateStoreTable, DatasetAdmin> {

  private final DatasetDefinition<? extends KeyValueTable, ?> tableDefinition;

  public StateStoreTableDefinition(String name, DatasetDefinition<? extends KeyValueTable, ?> tableDefinition) {
    super(name);
    this.tableDefinition = tableDefinition;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDefinition.configure("data", DatasetProperties.EMPTY))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return tableDefinition.getAdmin(spec.getSpecification("data"), classLoader);
  }

  @Override
  public StateStoreTable getDataset(DatasetSpecification spec, Map<String, String> arguments, ClassLoader classLoader)
    throws IOException {
    DatasetSpecification tableSpec = spec.getSpecification("data");
    KeyValueTable table = tableDefinition.getDataset(tableSpec, arguments, classLoader);
    return new StateStoreTableDataset(spec, table);
  }
}
