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

package co.cask.cdap.shell.command.deploy;

import co.cask.cdap.client.DatasetModuleClient;
import co.cask.cdap.shell.command.Command;
import co.cask.cdap.shell.command.CommandSet;
import com.google.common.collect.Lists;

import javax.inject.Inject;

/**
 * Contains commands for deploying dataset stuff.
 */
public class DeployDatasetCommandSet extends CommandSet {

  @Inject
  public DeployDatasetCommandSet(DatasetModuleClient datasetModuleClient) {
    super("dataset", Lists.<Command>newArrayList(
      new DeployDatasetModuleCommand(datasetModuleClient)
    ));
  }
}
