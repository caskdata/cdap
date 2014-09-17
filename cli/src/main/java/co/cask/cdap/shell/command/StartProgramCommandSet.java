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

package co.cask.cdap.shell.command;

import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.shell.CommandSet;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.HasCommand;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.util.List;

/**
 * Contains commands for starting programs.
 */
public class StartProgramCommandSet extends CommandSet {

  @Inject
  public StartProgramCommandSet(ProgramClient programClient) {
    super(generateCommands(programClient));
  }

  private static List<HasCommand> generateCommands(ProgramClient programClient) {
    List<HasCommand> commands = Lists.newArrayList();
    for (ElementType elementType : ElementType.values()) {
      if (elementType.canStartStop()) {
        commands.add(new StartProgramCommand(elementType, programClient));
      }
    }
    return commands;
  }
}
