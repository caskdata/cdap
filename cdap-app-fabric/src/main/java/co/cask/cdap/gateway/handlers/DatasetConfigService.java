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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.config.ConfigService;
import co.cask.cdap.app.config.ConfigType;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * ConfigService implementation using Dataset.
 */
public class DatasetConfigService extends AbstractIdleService implements ConfigService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetConfigService.class);
  private static final String RECENT_DASHBOARD_ID = "idcount";

  private final DatasetFramework dsFramework;
  private final TransactionExecutorFactory executorFactory;
  private Table configTable;
  private Table dashboardTable;
  private KeyValueTable metaDataTable;
  private TransactionExecutor executor;

  @Inject
  public DatasetConfigService(CConfiguration cConf, DatasetFramework dsFramework,
                              TransactionExecutorFactory executorFactory) {
    this.dsFramework = new NamespacedDatasetFramework(dsFramework, new DefaultDatasetNamespace(cConf,
                                                                                               Namespace.SYSTEM));
    this.executorFactory = executorFactory;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting DatasetConfigService...");
    //ConfigTable - stores the configurations (as row keys) and their settings as column:values
    configTable = DatasetsUtil.getOrCreateDataset(dsFramework, Constants.ConfigService.CONFIG_STORE_TABLE,
                                                  Table.class.getName(), DatasetProperties.EMPTY, null, null);
    //DashboardTable - stores the users (as row key) and the dashboards created by them as columns.
    dashboardTable = DatasetsUtil.getOrCreateDataset(dsFramework, Constants.ConfigService.DASHBOARD_OWNER_TABLE,
                                                     Table.class.getName(), DatasetProperties.EMPTY, null, null);
    //MetaDataTable - stores monotonically increasing count for dashboard-id of each namespace.
    //It also stores the dashboard configurations as rows (created when a dashboard is created and deleted when the
    //dashboard is deleted.
    metaDataTable = DatasetsUtil.getOrCreateDataset(dsFramework, Constants.ConfigService.METADATA_TABLE,
                                                    KeyValueTable.class.getName(), DatasetProperties.EMPTY, null, null);
    List<TransactionAware> txList = Lists.newArrayList();
    txList.addAll(ImmutableList.of(metaDataTable, (TransactionAware) configTable, (TransactionAware) dashboardTable));
    executor = executorFactory.createExecutor(txList);
    Preconditions.checkNotNull(configTable, "Could not get/create ConfigTable");
    Preconditions.checkNotNull(dashboardTable, "Could not get/create DashboardTable");
    Preconditions.checkNotNull(metaDataTable, "Could not get/create MetaDataTable");
    Preconditions.checkNotNull(executor, "Transaction Executor is null");
    LOG.info("Started DatasetConfigService...");
  }

  @Override
  protected void shutDown() {
    closeDataSet(configTable);
    closeDataSet(dashboardTable);
    closeDataSet(metaDataTable);
  }

  private void closeDataSet(Closeable ds) {
    try {
      ds.close();
    } catch (Throwable t) {
      LOG.error("Dataset throws exceptions during close:" + ds.toString(), t);
    }
  }

  @Override
  public void writeSetting(final String namespace, final ConfigType type, final String name, final String key,
                           final String value)
    throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        configTable.put(getRowKey(namespace, type, name), Bytes.toBytes(key), Bytes.toBytes(value));
      }
    });
  }

  @Override
  public void writeSetting(final String namespace, final ConfigType type, final String name,
                           final Map<String, String> settingsMap)
    throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (Map.Entry<String, String> setting : settingsMap.entrySet()) {
          configTable.put(getRowKey(namespace, type, name), Bytes.toBytes(setting.getKey()),
                          Bytes.toBytes(setting.getValue()));
        }
      }
    });
  }

  @Override
  public String readSetting(final String namespace, final ConfigType type, final String name, final String key)
    throws Exception {
    return executor.execute(new TransactionExecutor.Function<Void, String>() {
      @Override
      public String apply(Void o) throws Exception {
        return Bytes.toString(configTable.get(getRowKey(namespace, type, name), Bytes.toBytes(key)));
      }
    }, null);
  }

  @Override
  public Map<String, String> readSetting(final String namespace, final ConfigType type, final String name)
    throws Exception {
    return executor.execute(new TransactionExecutor.Function<Void, Map<String, String>>() {
      @Override
      public Map<String, String> apply(Void i) throws Exception {
        Map<String, String> settings = Maps.newHashMap();
        byte[] rowKey = getRowKey(namespace, type, name);
        for (Map.Entry<byte[], byte[]> entry : configTable.get(rowKey).getColumns().entrySet()) {
          settings.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
        }
        return settings;
      }
    }, null);
  }

  @Override
  public void deleteSetting(final String namespace, final ConfigType type, final String name, final String key)
    throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        configTable.delete(getRowKey(namespace, type, name), Bytes.toBytes(key));
      }
    });
  }

  @Override
  public void deleteSetting(final String namespace, final ConfigType type, final String name) throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        configTable.delete(getRowKey(namespace, type, name));
      }
    });
  }

  @Override
  public void deleteConfig(final String namespace, final ConfigType type, final String accId, final String name)
    throws Exception {
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        configTable.delete(getRowKey(namespace, type, name));
        if (type == ConfigType.DASHBOARD) {
          //Delete the dashboard entry from the dashboard ownership table and the metaDataTable
          dashboardTable.delete(Bytes.toBytes(accId), getRowKey(namespace, type, name));
          metaDataTable.delete(getRowKey(namespace, type, name));
        }
      }
    });
  }

  @Override
  public String createConfig(final String namespace, final ConfigType type, final String accId) throws Exception {
    final String countId = String.format("namespace.%s.%s", namespace, RECENT_DASHBOARD_ID);
    return executor.execute(new TransactionExecutor.Function<Void, String>() {
      @Override
      public String apply(Void i) throws Exception {
        if (type != ConfigType.DASHBOARD) {
          return null;
        }
        byte[] value = metaDataTable.read(countId);
        //Get the next id for the given namespace
        Long id =  (value == null) ? 0 : (Bytes.toLong(value) + 1);
        String dashboardId = Long.toString(id);
        //Add the dashboard id to the list of dashboards owned by the user
        dashboardTable.put(Bytes.toBytes(accId), getRowKey(namespace, type, dashboardId), Bytes.toBytes(true));
        //Update the latest dashboard-id for the given namespace
        metaDataTable.write(countId, Bytes.toBytes(id));
        //Make an entry in the metaDataTable to indicate the creation of this dashboard
        metaDataTable.write(getRowKey(namespace, type, dashboardId), Bytes.toBytes(true));
        return dashboardId;
      }
    }, null);
  }

  @Override
  public List<String> getConfig(final String namespace, final ConfigType type, final String accId) throws Exception {
    final List<String> configs = Lists.newArrayList();
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        if (type == ConfigType.USER) {
          configs.add(accId);
        } else if (type == ConfigType.DASHBOARD) {
          //Get all the dashboards owned by the user
          for (Map.Entry<byte[], byte[]> entry : dashboardTable.get(Bytes.toBytes(accId)).getColumns().entrySet()) {
            String column = Bytes.toString(entry.getKey());
            //Select only the ones that belong to the given namespace
            if (column.startsWith(getRowKeyString(namespace, type, null))) {
              //Extract the dashboard id
              configs.add(column.substring(column.lastIndexOf(".") + 1));
            }
          }
        }
      }
    });
    return configs;
  }

  @Override
  public List<String> getConfig(final String namespace, final ConfigType type) throws Exception {
    final List<String> configs = Lists.newArrayList();
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        //Scan the metaDataTable for rows that have keys corresponding to the given namespace
        byte[] startRowPrefix = getRowKey(namespace, type, null);
        byte[] endRowPrefix = Bytes.stopKeyForPrefix(startRowPrefix);
        CloseableIterator<KeyValue<byte[], byte[]>> iterator = metaDataTable.scan(startRowPrefix, endRowPrefix);
        while (iterator.hasNext()) {
          KeyValue<byte[], byte[]> entry = iterator.next();
          String rowKey = Bytes.toString(entry.getKey());
          //Extract the dashboard id
          configs.add(rowKey.substring(rowKey.lastIndexOf('.') + 1));
        }
        iterator.close();
      }
    });
    return configs;
  }

  @Override
  public boolean checkConfig(final String namespace, final ConfigType type, final String name) throws Exception {
    return executor.execute(new TransactionExecutor.Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void i) throws Exception {
        if (type != ConfigType.DASHBOARD) {
          return true;
        }
        byte[] value = (metaDataTable.read(getRowKey(namespace, type, name)));
        return (value != null);
      }
    }, null);
  }

  private String getRowKeyString(String namespace, ConfigType type, String name) {
    String rowKeyString = null;
    if (ConfigType.DASHBOARD == type) {
      rowKeyString = String.format("namespace.%s.dashboard.", namespace);
      if (name != null) {
        rowKeyString += name;
      }
    } else if (ConfigType.USER == type) {
      rowKeyString = String.format("user.%s", name);
    }
    return rowKeyString;
  }

  private byte[] getRowKey(String namespace, ConfigType type, String name) {
    return Bytes.toBytes(getRowKeyString(namespace, type, name));
  }
}
