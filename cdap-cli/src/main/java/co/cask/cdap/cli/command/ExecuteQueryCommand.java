/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AsciiTable;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * Executes a dataset query.
 */
public class ExecuteQueryCommand implements Command {

  private static final long TIMEOUT_MS = 30000;
  private final QueryClient queryClient;

  @Inject
  public ExecuteQueryCommand(QueryClient queryClient) {
    this.queryClient = queryClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String prefix = "execute query ";
    String query = arguments.getRawInput().substring(prefix.length());

    QueryHandle queryHandle = queryClient.execute(query);
    QueryStatus status = null;

    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < TIMEOUT_MS) {
      status = queryClient.getStatus(queryHandle);
      if (status.getStatus().isDone()) {
        break;
      }
      Thread.sleep(1000);
    }

    if (status != null && status.hasResults()) {
      final List<ColumnDesc> schema = queryClient.getSchema(queryHandle);
      String[] header = new String[schema.size()];
      for (int i = 0; i < header.length; i++) {
        ColumnDesc column = schema.get(i);
        // Hive columns start at 1
        int index = column.getPosition() - 1;
        header[index] = column.getName() + ": " + column.getType();
      }
      List<QueryResult> results = queryClient.getResults(queryHandle, 20);

      new AsciiTable<QueryResult>(header, results, new RowMaker<QueryResult>() {
        @Override
        public Object[] makeRow(QueryResult object) {
          return convertRow(object.getColumns());
        }
      }).print(output);

      queryClient.delete(queryHandle);
    } else {
      output.println("Couldn't obtain results after " + (System.currentTimeMillis() - startTime) + "ms. " +
                       "Try querying manually with handle " + queryHandle.getHandle());
    }
  }

  private Object[] convertRow(List<QueryResult.ResultObject> row) {
    Object[] result = new Object[row.size()];
    for (int index = 0; index < row.size(); index++) {
      result[index] = row.get(index).getObject();
    }
    return result;
  }

  @Override
  public String getPattern() {
    return String.format("execute <%s>", ArgumentName.QUERY);
  }

  @Override
  public String getDescription() {
    return "Executes a " + ElementType.QUERY.getPrettyName();
  }
}
