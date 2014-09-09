/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.shell.command.set;

import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.command.AbstractCommand;
import co.cask.cdap.shell.command.ArgumentName;
import co.cask.cdap.shell.command.Arguments;
import co.cask.cdap.shell.command.Command;

import java.io.PrintStream;
import javax.inject.Inject;

/**
 * Sets the instances of a program.
 */
public class SetProgramInstancesCommand extends AbstractCommand {

  private final ProgramClient programClient;
  private final ElementType elementType;

  public SetProgramInstancesCommand(ElementType elementType, ProgramClient programClient) {
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(ArgumentName.PROGRAM).split("\\.");
    String appId = programIdParts[0];
    int numInstances = arguments.getInt(ArgumentName.NUM_INSTANCES);

    switch (elementType) {
      case FLOWLET:
        String flowId = programIdParts[1];
        String flowletId = programIdParts[2];
        programClient.setFlowletInstances(appId, flowId, flowletId, numInstances);
        output.printf("Successfully set flowlet '%s' of flow '%s' of app '%s' to %d instances\n",
                      flowId, flowletId, appId, numInstances);
        break;
      case PROCEDURE:
        String procedureId = programIdParts[1];
        programClient.setProcedureInstances(appId, procedureId, numInstances);
        output.printf("Successfully set procedure '%s' of app '%s' to %d instances\n",
                      procedureId, appId, numInstances);
        break;
      case RUNNABLE:
        String serviceId = programIdParts[1];
        String runnableId = programIdParts[2];
        programClient.setServiceRunnableInstances(appId, serviceId, runnableId, numInstances);
        output.printf("Successfully set runnable '%s' of service '%s' of app '%s' to %d instances\n",
                      runnableId, serviceId, appId, numInstances);
        break;
      default:
        // TODO: remove this
        throw new IllegalArgumentException("Unrecognized program element type for scaling: " + elementType);
    }
  }

  @Override
  public String getPattern() {
    return String.format("set %s instances <%s> <%s>", elementType.getName(),
                         ArgumentName.PROGRAM, ArgumentName.NUM_INSTANCES);
  }

  @Override
  public String getDescription() {
    return "Sets the instances of a " + elementType.getPrettyName();
  }
}
