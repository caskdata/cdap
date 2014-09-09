/*
 * Copyright 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.shell.command.common;

import co.cask.cdap.shell.CLIConfig;
import co.cask.cdap.shell.Constants;
import co.cask.cdap.shell.command.AbstractCommand;
import co.cask.cdap.shell.command.Arguments;
import co.cask.cdap.shell.command.Command;
import co.cask.cdap.shell.command.CommandSet;
import com.google.common.base.Supplier;

import java.io.PrintStream;

/**
 * Prints helper text for all commands.
 */
public class HelpCommand extends AbstractCommand {

  private final Supplier<CommandSet> getCommands;
  private final CLIConfig config;

  public HelpCommand(Supplier<CommandSet> getCommands, CLIConfig config) {
    this.getCommands = getCommands;
    this.config = config;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    output.println("CLI version " + config.getVersion());
    output.println(Constants.ENV_HOSTNAME + "=" + config.getHost());
    output.println();
    output.println("Available commands: \n" + getCommands.get().getHelperText());
  }

  @Override
  public String getPattern() {
    return "help";
  }

  @Override
  public String getDescription() {
    return "Prints this helper text";
  }
}
