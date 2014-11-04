/*
 * *
 *  Copyright © 2014 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 * /
 */

package co.cask.cdap.test.app;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;

/**
 * Simple Dataset with version
 */

public class VersionedDataset extends AbstractDataset {

  public VersionedDataset(DatasetSpecification spec,
                          @EmbeddedDataset("unique") KeyValueTable uniqueCountTable) {
    super(spec.getName(), uniqueCountTable);
  }

  @Override
  public int getVersion() {
    return 1;
  }
}

