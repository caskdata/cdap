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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.metrics.MeteredDataset;
import com.continuuity.tephra.Transaction;
import com.continuuity.tephra.TransactionAware;
import com.continuuity.tephra.TransactionAwares;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collection;

/**
 * Handy abstract implementation of {@link Dataset} that acts on a list of underlying datasets and
 * implements {@link TransactionAware} and {@link MeteredDataset} interfaces by propagating corresponded
 * logic to each dataset in a list when possible.
 */
public abstract class AbstractDataset implements Dataset, MeteredDataset, TransactionAware {
  private final String instanceName;
  private final Collection<Dataset> underlying;
  private final TransactionAware txAwares;

  public AbstractDataset(String instanceName, Dataset embedded, Dataset... otherEmbedded) {
    this.instanceName = instanceName;
    this.underlying = Lists.asList(embedded, otherEmbedded);
    ImmutableList.Builder<TransactionAware> builder = ImmutableList.builder();
    for (Dataset dataset : underlying) {
      if (dataset instanceof TransactionAware) {
        builder.add((TransactionAware) dataset);
      }
    }
    this.txAwares = TransactionAwares.of(builder.build());
  }

  @Override
  public void close() throws IOException {
    for (Dataset dataset : underlying) {
      dataset.close();
    }
  }

  // metering stuff

  @Override
  public void setMetricsCollector(MeteredDataset.MetricsCollector metricsCollector) {
    for (Dataset dataset : underlying) {
      if (dataset instanceof MeteredDataset) {
        ((MeteredDataset) dataset).setMetricsCollector(metricsCollector);
      }
    }
  }

  // transaction stuff

  @Override
  public void startTx(Transaction tx) {
    txAwares.startTx(tx);
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    return txAwares.getTxChanges();
  }

  @Override
  public boolean commitTx() throws Exception {
    return txAwares.commitTx();
  }

  @Override
  public void postTxCommit() {
    txAwares.postTxCommit();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    return txAwares.rollbackTx();
  }

  @Override
  public String getTransactionAwareName() {
    return instanceName;
  }

  protected String getName() {
    return instanceName;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Dataset");
    sb.append("{underlying=").append(underlying);
    sb.append(", instanceName='").append(instanceName).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
