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

import co.cask.cdap.client.StreamClient;
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.ArgumentName;
import co.cask.cdap.shell.Arguments;
import co.cask.cdap.shell.ElementType;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * Creates a stream.
 */
public class CreateStreamCommand extends AbstractCommand {

  private final StreamClient streamClient;

  @Inject
  public CreateStreamCommand(StreamClient streamClient) {
    this.streamClient = streamClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String streamId = arguments.get(ArgumentName.NEW_STREAM);

    streamClient.create(streamId);
    output.printf("Successfully created stream with ID '%s'\n", streamId);
  }

  @Override
  public String getPattern() {
    return String.format("create stream <%s>", ArgumentName.NEW_STREAM);
  }

  @Override
  public String getDescription() {
    return "Creates a " + ElementType.STREAM.getPrettyName();
  }
}
