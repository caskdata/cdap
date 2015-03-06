/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * Common utility methods for dealing with HBase table name conversions.
 */
public abstract class HTableNameConverter {
  protected static final String HBASE_NAMESPACE_PREFIX = "cdap_";

  private static String getHBaseTableName(String tableName) {
    return encodeTableName(tableName);
  }

  private static String encodeTableName(String tableName) {
    try {
      return URLEncoder.encode(tableName, "ASCII");
    } catch (UnsupportedEncodingException e) {
      // this can never happen - we know that ASCII is a supported character set!
      throw new RuntimeException(e);
    }
  }

  private static String getBackwardCompatibleTableName(String tablePrefix, TableId tableId) {
    String tableName = tableId.getTableName();
    // handle table names in default namespace so we do not have to worry about upgrades
    if (Constants.DEFAULT_NAMESPACE_ID.equals(tableId.getNamespace())) {
      // if the table name starts with 'system.', then its a queue or stream table. Do not add namespace to table name
      // e.g. namespace = default, tableName = system.queue.config. Resulting table name = cdap.system.queue.config
      // also no need to prepend the table name if it already starts with 'user'.
      // TODO: the 'user' should be prepended by the HBaseTableAdmin.
      if (tableName.startsWith(String.format("%s.", Constants.SYSTEM_NAMESPACE)) ||
          tableName.startsWith("user.")) {
        return Joiner.on(".").join(tablePrefix, tableName);
      }
      // if the table name does not start with 'system.', then its a user dataset. Add 'user' to the table name to
      // maintain backward compatibility. Also, do not add namespace to the table name
      // e.g. namespace = default, tableName = purchases. Resulting table name = cdap.user.purchases
      return Joiner.on(".").join(tablePrefix, "user", tableName);
    }
    // if the namespace is not default, do not need to change anything
    return tableName;
  }

  /**
   * @return Backward compatible, ASCII encoded table name
   */
  protected static String getHBaseTableName(CConfiguration cConf, TableId tableId) {
    String tablePrefix = cConf.get(Constants.Dataset.TABLE_PREFIX);
    Preconditions.checkArgument(tablePrefix != null, "Table prefix should not be null.");
    return getHBaseTableName(getBackwardCompatibleTableName(tablePrefix, tableId));
  }

  /**
   * Gets the system configuration table prefix
   * @param hTableName Full HBase table name.
   * @return System configuration table prefix (full table name minus the table qualifier).
   * Example input: "cdap_ns.table.name"  -->  output: "cdap_system."   (hbase 94)
   * Example input: "cdap.table.name"     -->  output: "cdap_system."   (hbase 94. input table is in default namespace)
   * Example input: "cdap_ns:table.name"  -->  output: "cdap_system:"   (hbase 96, 98)
   */
  public abstract String getSysConfigTablePrefix(String hTableName);

  @VisibleForTesting
  protected static String toHBaseNamespace(Id.Namespace namespace) {
    // Handle backward compatibility to not add the prefix for default namespace
    // TODO: CDAP-1601 - Conditional should be removed when we have a way to upgrade user datasets
    return HTableNameConverter.getHBaseTableName(Constants.DEFAULT_NAMESPACE_ID.equals(namespace) ? namespace.getId() :
                                                   HBASE_NAMESPACE_PREFIX + namespace.getId());
  }

  protected static TableId from(String hBaseNamespace, String hTableName) {
    Preconditions.checkArgument(hBaseNamespace != null, "Table namespace should not be null.");
    Preconditions.checkArgument(hTableName != null, "Table name should not be null.");

    String namespace;
    String prefix;

    // Handle backward compatibility to not add the prefix for default namespace
    if (Constants.DEFAULT_NAMESPACE.equals(hBaseNamespace)) {
      namespace = hBaseNamespace;
      // in Default namespace, hTableName is something like 'cdap.foo.table'
      String[] parts = hTableName.split("\\.", 2);
      Preconditions.checkArgument(parts.length == 2,
                                  String.format("expected table name to contain '.': %s", hTableName));
      // prefix is parts[0], but it is not needed
      hTableName = parts[1];
      return TableId.from(namespace, hTableName);
    }


    String[] parts = hBaseNamespace.split("_");
    Preconditions.checkArgument(parts.length == 2,
                                String.format("expected hbase namespace to have a '_': %s", hBaseNamespace));
    // prefix is parts[0], but it is not needed
    namespace = parts[1];

    // Id.Namespace already checks for non-null namespace
    return TableId.from(namespace, hTableName);
  }
}
