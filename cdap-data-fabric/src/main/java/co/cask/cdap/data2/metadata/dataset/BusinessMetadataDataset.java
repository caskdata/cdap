/*
 * Copyright 2015 Cask Data, Inc.
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
package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.Id;

import co.cask.cdap.proto.MetadataSearchTargetType;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Implementation of Business Metadata on top of {@link IndexedTable}.
 */
public class BusinessMetadataDataset extends AbstractDataset {
  // column keys
  public static final String KEYVALUE_COLUMN = "kv";
  public static final String VALUE_COLUMN = "v";

  public static final String KEYVALUE_SEPARATOR = ":";

  private final IndexedTable indexedTable;

  public BusinessMetadataDataset(IndexedTable indexedTable) {
    super("businessMetadataIndexedTable", indexedTable);
    this.indexedTable = indexedTable;
  }

  /**
   * Add new business metadata.
   *
   * @param metadataRecord The value of the metadata to be saved.
   */
  public void createBusinessMetadata(BusinessMetadataRecord metadataRecord) {
    Id.NamespacedId targetId = metadataRecord.getTargetId();
    String key = metadataRecord.getKey();
    MDSKey mdsKey = getMDSKey(targetId, key);

    // Put to the default column.
    write(mdsKey, metadataRecord);
  }

  /**
   * Add new business metadata.
   *
   * @param targetId The target Id: app-id(ns+app) / program-id(ns+app+pgtype+pgm) /
   *                 dataset-id(ns+dataset)/stream-id(ns+stream).
   * @param key The metadata key to be added.
   * @param value The metadata value to be added.
   */
  public void createBusinessMetadata(Id.NamespacedId targetId, String key, String value) {
    createBusinessMetadata(new BusinessMetadataRecord(targetId, key, value));
  }

  /**
   * Return business metadata based on type, target id, and key.
   *
   * @param targetId The id of the target.
   * @param key The metadata key to get.
   * @return instance of {@link BusinessMetadataRecord} for the target type, id, and key.
   */
  @Nullable
  public BusinessMetadataRecord getBusinessMetadata(Id.NamespacedId targetId, String key) {
    MDSKey mdsKey = getMDSKey(targetId, key);
    Row row = indexedTable.get(mdsKey.getKey());
    if (row.isEmpty()) {
      return null;
    }

    byte[] value = row.get(VALUE_COLUMN);

    return new BusinessMetadataRecord(targetId, key, Bytes.toString(value));
  }

  /**
   * Retrieves the business metadata for the specified {@link Id.NamespacedId}.
   *
   * @param targetId the specified {@link Id.NamespacedId}
   * @return a Map representing the metadata for the specified {@link Id.NamespacedId}
   */
  public Map<String, String> getBusinessMetadata(Id.NamespacedId targetId) {
    String targetType = getTargetType(targetId);
    MDSKey mdsKey = getMDSKey(targetId, null);
    byte[] startKey = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);

    Map<String, String> metadata = new HashMap<>();
    Scanner scan = indexedTable.scan(startKey, stopKey);
    try {
      Row next;
      while ((next = scan.next()) != null) {
        String key = getMetadataKey(targetType, next.getRow());
        byte[] value = next.get(VALUE_COLUMN);
        if (key == null || value == null) {
          continue;
        }
        metadata.put(key, Bytes.toString(value));
      }
      return metadata;
    } finally {
      scan.close();
    }
  }

  /**
   * Removes all business metadata for the specified {@link Id.NamespacedId}.
   *
   * @param targetId the {@link Id.NamespacedId} for which metadata is to be removed.
   */
  public void removeMetadata(Id.NamespacedId targetId) {
    removeMetadata(targetId, Predicates.<String>alwaysTrue());
  }

  /**
   * Removes the specified keys from the business metadata of the specified {@link Id.NamespacedId}.
   *
   * @param targetId the {@link Id.NamespacedId} for which the specified metadata keys are to be removed.
   * @param keys the keys to remove from the metadata of the specified {@link Id.NamespacedId}
   */
  public void removeMetadata(Id.NamespacedId targetId, String ... keys) {
    final Set<String> keySet = Sets.newHashSet(keys);
    removeMetadata(targetId, new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return keySet.contains(input);
      }
    });
  }

  /**
   * Removes all keys that satisfy a given predicate from the metadata of the specified {@link Id.NamespacedId}.
   *
   * @param targetId the {@link Id.NamespacedId} for which keys are to be removed.
   * @param filter the {@link Predicate} that should be satisfied to remove a key.
   */
  public void removeMetadata(Id.NamespacedId targetId, Predicate<String> filter) {
    String targetType = getTargetType(targetId);
    MDSKey mdsKey = getMDSKey(targetId, null);
    byte[] prefix = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(prefix);

    Scanner scan = indexedTable.scan(prefix, stopKey);
    try {
      Row next;
      while ((next = scan.next()) != null) {
        String keyValue = next.getString(KEYVALUE_COLUMN);
        String value = next.getString(VALUE_COLUMN);
        if (keyValue == null && value == null) {
          continue;
        }
        if (filter.apply(getMetadataKey(targetType, next.getRow()))) {
          indexedTable.delete(new Delete(next.getRow()));
        }
      }
    } finally {
      scan.close();
    }
  }

  /**
   * Find the instance of {@link BusinessMetadataRecord} based on key.
   *
   * @param value The metadata value to be found.
   * @param type The target type of objects to search from.
   * @return The {@link Iterable} of {@link BusinessMetadataRecord} that fit the value.
   */
  public List<BusinessMetadataRecord> findBusinessMetadataOnValue(String value, MetadataSearchTargetType type) {
    return executeSearchOnColumns(BusinessMetadataDataset.VALUE_COLUMN, value, type);
  }

  /**
   * Find the instance of {@link BusinessMetadataRecord} for key:value pair
   *
   * @param keyValue The metadata value to be found.
   * @param type The target type of objects to search from.
   * @return The {@link Iterable} of {@link BusinessMetadataRecord} that fit the key value pair.
   */
  public List<BusinessMetadataRecord> findBusinessMetadataOnKeyValue(String keyValue, MetadataSearchTargetType type) {
    return executeSearchOnColumns(BusinessMetadataDataset.KEYVALUE_COLUMN, keyValue, type);
  }

  // Helper method to execute IndexedTable search on target index column.
  List<BusinessMetadataRecord> executeSearchOnColumns(String column, String searchValue,
                                                      MetadataSearchTargetType type) {
    List<BusinessMetadataRecord> results = new LinkedList<>();
    Scanner scanner = indexedTable.readByIndex(Bytes.toBytes(column), Bytes.toBytes(searchValue));
    try {
      Row next;
      while ((next = scanner.next()) != null) {
        String rowValue = next.getString(VALUE_COLUMN);
        if (rowValue == null) {
          continue;
        }

        final byte[] rowKey = next.getRow();
        String targetType = getTargetType(rowKey);

        // Filter on target type if not ALL
        if ((type != MetadataSearchTargetType.ALL) && (!targetType.equals(type.getInternalName()))) {
          continue;
        }

        Id.NamespacedId targetId = getNamespaceIdFromKey(targetType, new MDSKey(rowKey));
        String key = getMetadataKey(targetType, rowKey);
        String value = Bytes.toString(next.get(Bytes.toBytes(BusinessMetadataDataset.VALUE_COLUMN)));
        BusinessMetadataRecord record = new BusinessMetadataRecord(targetId, key, value);
        results.add(record);
      }
    } finally {
      scanner.close();
    }

    return results;
  }

  void addNamespaceIdToKey(final MDSKey.Builder builder, Id.NamespacedId namespacedId) {
    String type = getTargetType(namespacedId);
    if (type.equals(Id.Program.class.getSimpleName())) {
      Id.Program program = (Id.Program) namespacedId;
      String namespaceId = program.getNamespaceId();
      String appId = program.getApplicationId();
      String programType = program.getType().name();
      String programId = program.getId();
      builder.add(namespaceId);
      builder.add(appId);
      builder.add(programType);
      builder.add(programId);
    } else if (type.equals(Id.Application.class.getSimpleName())) {
      Id.Application application = (Id.Application) namespacedId;
      String namespaceId = application.getNamespaceId();
      String instanceId = application.getId();
      builder.add(namespaceId);
      builder.add(instanceId);
    } else if (type.equals(Id.DatasetInstance.class.getSimpleName())) {
      Id.DatasetInstance datasetInstance = (Id.DatasetInstance) namespacedId;
      String namespaceId = datasetInstance.getNamespaceId();
      String instanceId = datasetInstance.getId();
      builder.add(namespaceId);
      builder.add(instanceId);
    } else if (type.equals(Id.Stream.class.getSimpleName())) {
      Id.Stream stream = (Id.Stream) namespacedId;
      String namespaceId = stream.getNamespaceId();
      String instanceId = stream.getId();
      builder.add(namespaceId);
      builder.add(instanceId);
    } else {
      throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
    }
  }

  Id.NamespacedId getNamespaceIdFromKey(String type, MDSKey key) {
    MDSKey.Splitter keySplitter = key.split();

    // The rowkey is [targetType][targetId][key], so skip the first string.
    keySplitter.skipString();
    if (type.equals(Id.Program.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String appId = keySplitter.getString();
      String programType = keySplitter.getString();
      String programId = keySplitter.getString();
      return Id.Program.from(namespaceId, appId, ProgramType.valueOf(programType), programId);
    } else if (type.equals(Id.Application.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String appId = keySplitter.getString();
      return Id.Application.from(namespaceId, appId);
    } else if (type.equals(Id.DatasetInstance.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String instanceId  = keySplitter.getString();
      return Id.DatasetInstance.from(namespaceId, instanceId);
    } else if (type.equals(Id.Stream.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String instanceId  = keySplitter.getString();
      return Id.DatasetInstance.from(namespaceId, instanceId);
    }
    throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
  }

  private void write(MDSKey id, BusinessMetadataRecord record) {
    try {
      Put put = new Put(id.getKey());

      // Now add the index columns.
      put.add(Bytes.toBytes(KEYVALUE_COLUMN), Bytes.toBytes(record.getKey() + KEYVALUE_SEPARATOR + record.getValue()));
      put.add(Bytes.toBytes(VALUE_COLUMN), Bytes.toBytes(record.getValue()));

      indexedTable.put(put);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // Helper method to generate key.
  private MDSKey getMDSKey(Id.NamespacedId targetId, @Nullable String key) {
    String targetType = getTargetType(targetId);
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(targetType);
    addNamespaceIdToKey(builder, targetId);
    if (key != null) {
      builder.add(key);
    }

    return builder.build();
  }

  private String getTargetType(Id.NamespacedId namespacedId) {
    if (namespacedId instanceof Id.Program) {
      return Id.Program.class.getSimpleName();
    }
    return namespacedId.getClass().getSimpleName();
  }

  private String getMetadataKey(String type, byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();
    // The rowkey is [targetType][targetId][key], so skip the first few strings.
    keySplitter.skipString();
    if (type.equals(Id.Program.class.getSimpleName())) {
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(Id.Application.class.getSimpleName())) {
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(Id.DatasetInstance.class.getSimpleName())) {
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(Id.Stream.class.getSimpleName())) {
      keySplitter.skipString();
      keySplitter.skipString();
    } else {
      throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
    }
    return keySplitter.getString();
  }

  private String getTargetType(byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();
    // The rowkey is [targetType][targetId][key]
    return keySplitter.getString();
  }
}
