/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store;

import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.internal.github.GitHubRepo;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class GitHubStore {
  private final TransactionRunner transactionRunner;
  @Inject
  public GitHubStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  public void addOrUpdateRepo(String nickname, String url, String defaultBranch,
           String authString) throws IOException {
    TransactionRunners.run(transactionRunner, context ->  {
      StructuredTable table = context.getTable(StoreDefinition.GitHubStore.GIT_REPOS);
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.GitHubStore.NICKNAME_FIELD, nickname));
      fields.add(Fields.stringField(StoreDefinition.GitHubStore.URL_FIELD, url));
      fields.add(Fields.stringField(StoreDefinition.GitHubStore.DEFAULT_BRANCH_FIELD, defaultBranch));
      fields.add(Fields.stringField(StoreDefinition.GitHubStore.AUTH_STRING_FIELD, authString));
      table.upsert(fields);
    }, IOException.class);

  }

  public void deleteRepo(String nickname) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.GitHubStore.GIT_REPOS);
      table.delete(Collections.singleton(Fields.stringField(StoreDefinition.GitHubStore.NICKNAME_FIELD, nickname)));
    }, IOException.class);
  }

  public GitHubRepo getRepo(String nickname) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.GitHubStore.GIT_REPOS);
      Optional<StructuredRow> out = table.read(Collections.singleton(
          Fields.stringField(StoreDefinition.GitHubStore.NICKNAME_FIELD, nickname)));
      String nName = out.get().getString(StoreDefinition.GitHubStore.NICKNAME_FIELD);
      String url = out.get().getString(StoreDefinition.GitHubStore.URL_FIELD);
      String defBranch = out.get().getString(StoreDefinition.GitHubStore.DEFAULT_BRANCH_FIELD);
      String aString = out.get().getString(StoreDefinition.GitHubStore.AUTH_STRING_FIELD);
      return new GitHubRepo(nName, url, defBranch, aString);
    }, IOException.class);
  }

  public List<GitHubRepo> getRepos() throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.GitHubStore.GIT_REPOS);
      List<GitHubRepo> repos = new ArrayList<>();
      CloseableIterator<StructuredRow> repoIterator = table.scan(Range.all(), Integer.MAX_VALUE);
      repoIterator.forEachRemaining(structuredRow -> {
        String nName = structuredRow.getString(StoreDefinition.GitHubStore.NICKNAME_FIELD);
        String url = structuredRow.getString(StoreDefinition.GitHubStore.URL_FIELD);
        String defBranch = structuredRow.getString(StoreDefinition.GitHubStore.DEFAULT_BRANCH_FIELD);
        String aString = structuredRow.getString(StoreDefinition.GitHubStore.AUTH_STRING_FIELD);
        repos.add(new GitHubRepo(nName, url, defBranch, aString));
      });
      return repos;
    }, IOException.class);
  }
}
