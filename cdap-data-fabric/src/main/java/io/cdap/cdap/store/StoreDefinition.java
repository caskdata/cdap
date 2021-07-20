/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.store;

import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Fields;

import java.io.IOException;

/**
 * A class which contains all the store definition, the table name the store will use, the schema of the table should
 * all be specified here.
 * TODO: CDAP-14674 Make sure all the store definition goes here.
 */
public final class StoreDefinition {

  private StoreDefinition() {
    // prevent instantiation
  }

  /**
   * Create all system tables. A boolean flag can be used to skip creating tables that already exist.
   *
   * @param tableAdmin the table admin to create the table
   * @param registry the registry to get initialized
   * @param overWrite true to create tables no matter the tables exist or not, false to skip tables that already exist
   */
  public static void createAllTables(StructuredTableAdmin tableAdmin, StructuredTableRegistry registry,
                                     boolean overWrite) throws IOException, TableAlreadyExistsException {
    registry.initialize();
    ArtifactStore.createTables(tableAdmin, overWrite);
    OwnerStore.createTables(tableAdmin, overWrite);
    NamespaceStore.createTable(tableAdmin, overWrite);
    SecretStore.createTable(tableAdmin, overWrite);
    WorkflowStore.createTables(tableAdmin, overWrite);
    ConfigStore.createTable(tableAdmin, overWrite);
    PreferencesStore.createTable(tableAdmin, overWrite);
    ProvisionerStore.createTable(tableAdmin, overWrite);
    AppMetadataStore.createTables(tableAdmin, overWrite);
    ProfileStore.createTables(tableAdmin, overWrite);
    ProgramScheduleStore.createTables(tableAdmin, overWrite);
    DatasetInstanceStore.createTables(tableAdmin, overWrite);
    DatasetTypeStore.createTables(tableAdmin, overWrite);
    LineageStore.createTable(tableAdmin, overWrite);
    JobQueueStore.createTables(tableAdmin, overWrite);
    TimeScheduleStore.createTables(tableAdmin, overWrite);
    RemoteRuntimeStore.createTables(tableAdmin, overWrite);
    ProgramHeartbeatStore.createTables(tableAdmin, overWrite);
    LogCheckpointStore.createTable(tableAdmin, overWrite);
    UsageStore.createTables(tableAdmin, overWrite);
    FieldLineageStore.createTables(tableAdmin, overWrite);
    LogFileMetaStore.createTables(tableAdmin, overWrite);
    CapabilitiesStore.createTable(tableAdmin, overWrite);
    GitHubStore.createTable(tableAdmin, overWrite);
  }

  public static void createAllTables(StructuredTableAdmin tableAdmin, StructuredTableRegistry registry)
    throws IOException, TableAlreadyExistsException {
    createAllTables(tableAdmin, registry, false);
  }

  public static final class GitHubStore {
    public static final StructuredTableId GIT_REPOS = new StructuredTableId("GitRepos");

    public static final String NICKNAME_FIELD = "nickname";
    public static final String URL_FIELD = "url";
    public static final String DEFAULT_BRANCH_FIELD = "defaultBranch";
    public static final String AUTH_STRING_FIELD = "authString";


    public static final StructuredTableSpecification GITHUB_TABLE_SPEC =
        new StructuredTableSpecification.Builder()
          .withId(GIT_REPOS)
          .withFields(Fields.stringType(NICKNAME_FIELD),
                      Fields.stringType(URL_FIELD),
                      Fields.stringType(DEFAULT_BRANCH_FIELD),
                      Fields.stringType(AUTH_STRING_FIELD))
          .withPrimaryKeys(NICKNAME_FIELD)
          .build();

    public static void createTable(StructuredTableAdmin tableAdmin,
                                   boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(GIT_REPOS) == null) {
        tableAdmin.create(GITHUB_TABLE_SPEC);
      }
    }
  }

  /**
   * Namespace store schema
   */
  public static final class NamespaceStore {
    public static final StructuredTableId NAMESPACES = new StructuredTableId("namespaces");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String NAMESPACE_METADATA_FIELD = "namespace_metadata";

    public static final StructuredTableSpecification NAMESPACE_TABLE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(NAMESPACES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(NAMESPACE_METADATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD)
        .build();

    public static void createTable(StructuredTableAdmin tableAdmin,
                                   boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(NAMESPACES) == null) {
        tableAdmin.create(NAMESPACE_TABLE_SPEC);
      }
    }
  }

  /**
   * Schema for ConfigStore
   */
  public static final class ConfigStore {
    public static final StructuredTableId CONFIGS = new StructuredTableId("configs");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String TYPE_FIELD = "type";
    public static final String NAME_FIELD = "name";
    public static final String PROPERTIES_FIELD = "properties";

    public static final StructuredTableSpecification CONFIG_TABLE_SPEC = new StructuredTableSpecification.Builder()
      .withId(CONFIGS)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(TYPE_FIELD),
                  Fields.stringType(NAME_FIELD),
                  Fields.stringType(PROPERTIES_FIELD))
      .withPrimaryKeys(NAMESPACE_FIELD, TYPE_FIELD, NAME_FIELD)
      .build();

    public static void createTable(StructuredTableAdmin tableAdmin,
                                   boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(CONFIGS) == null) {
        tableAdmin.create(CONFIG_TABLE_SPEC);
      }
    }
  }

  /**
   * Schema for ConfigStore
   */
  public static final class PreferencesStore {
    public static final StructuredTableId PREFERENCES = new StructuredTableId("preferences");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String TYPE_FIELD = "type";
    public static final String NAME_FIELD = "name";
    public static final String PROPERTIES_FIELD = "properties";
    public static final String SEQUENCE_ID_FIELD = "seq";

    public static final StructuredTableSpecification PREFERENCES_TABLE_SPEC = new StructuredTableSpecification.Builder()
      .withId(PREFERENCES)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(TYPE_FIELD),
                  Fields.stringType(NAME_FIELD),
                  Fields.stringType(PROPERTIES_FIELD),
                  Fields.longType(SEQUENCE_ID_FIELD))
      .withPrimaryKeys(NAMESPACE_FIELD, TYPE_FIELD, NAME_FIELD)
      .build();

    public static void createTable(StructuredTableAdmin tableAdmin,
                                   boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(PREFERENCES) == null) {
        tableAdmin.create(PREFERENCES_TABLE_SPEC);
      }
    }
  }

  /**
   * Schema for workflow table
   */
  public static final class WorkflowStore {
    public static final StructuredTableId WORKFLOW_STATISTICS = new StructuredTableId("workflow_statistics");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String PROGRAM_FIELD = "program";
    public static final String START_TIME_FIELD = "start_time";
    public static final String RUN_ID_FIELD = "run_id";
    public static final String TIME_TAKEN_FIELD = "time_taken";
    public static final String PROGRAM_RUN_DATA = "program_run_data";

    public static final StructuredTableSpecification WORKFLOW_TABLE_SPEC = new StructuredTableSpecification.Builder()
      .withId(WORKFLOW_STATISTICS)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(APPLICATION_FIELD),
                  Fields.stringType(VERSION_FIELD),
                  Fields.stringType(PROGRAM_FIELD),
                  Fields.longType(START_TIME_FIELD),
                  Fields.stringType(RUN_ID_FIELD),
                  Fields.longType(TIME_TAKEN_FIELD),
                  Fields.stringType(PROGRAM_RUN_DATA))
      .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_FIELD, START_TIME_FIELD)
      .build();

    public static void createTables(StructuredTableAdmin tableAdmin,
                                    boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(WORKFLOW_STATISTICS) == null) {
        tableAdmin.create(WORKFLOW_TABLE_SPEC);
      }
    }
  }

  /**
   *
   */
  public static final class ArtifactStore {
    public static final StructuredTableId ARTIFACT_DATA_TABLE = new StructuredTableId("artifact_data");
    public static final StructuredTableId APP_DATA_TABLE = new StructuredTableId("app_data");
    public static final StructuredTableId PLUGIN_DATA_TABLE = new StructuredTableId("plugin_data");
    public static final StructuredTableId UNIV_PLUGIN_DATA_TABLE = new StructuredTableId("universal_plugin_data");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String ARTIFACT_NAMESPACE_FIELD = "artifact_namespace";
    public static final String ARTIFACT_NAME_FIELD = "artifact_name";
    public static final String ARTIFACT_VER_FIELD = "artifiact_version";
    public static final String ARTIFACT_DATA_FIELD = "artifact_data";
    public static final String CLASS_NAME_FIELD = "class_name";
    public static final String APP_DATA_FIELD = "app_data";
    public static final String PARENT_NAMESPACE_FIELD = "parent_namespace";
    public static final String PARENT_NAME_FIELD = "parent_name";
    public static final String PLUGIN_TYPE_FIELD = "plugin_type";
    public static final String PLUGIN_NAME_FIELD = "plugin_name";
    public static final String PLUGIN_DATA_FIELD = "plugin_data";

    // Artifact Data table
    public static final StructuredTableSpecification ARTIFACT_DATA_SPEC = new StructuredTableSpecification.Builder()
      .withId(ARTIFACT_DATA_TABLE)
      .withFields(Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                  Fields.stringType(ARTIFACT_NAME_FIELD),
                  Fields.stringType(ARTIFACT_VER_FIELD),
                  Fields.stringType(ARTIFACT_DATA_FIELD))
      .withPrimaryKeys(ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD, ARTIFACT_VER_FIELD)
      .build();

    // App Data table
    public static final StructuredTableSpecification APP_DATA_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(APP_DATA_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(CLASS_NAME_FIELD),
                    Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                    Fields.stringType(ARTIFACT_NAME_FIELD),
                    Fields.stringType(ARTIFACT_VER_FIELD),
                    Fields.stringType(APP_DATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, CLASS_NAME_FIELD, ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD,
                         ARTIFACT_VER_FIELD)
        .build();

    // Plugin Data table
    public static final StructuredTableSpecification PLUGIN_DATA_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(PLUGIN_DATA_TABLE)
        .withFields(Fields.stringType(PARENT_NAMESPACE_FIELD),
                    Fields.stringType(PARENT_NAME_FIELD),
                    Fields.stringType(PLUGIN_TYPE_FIELD),
                    Fields.stringType(PLUGIN_NAME_FIELD),
                    Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                    Fields.stringType(ARTIFACT_NAME_FIELD),
                    Fields.stringType(ARTIFACT_VER_FIELD),
                    Fields.stringType(PLUGIN_DATA_FIELD))
        .withPrimaryKeys(PARENT_NAMESPACE_FIELD, PARENT_NAME_FIELD, PLUGIN_TYPE_FIELD, PLUGIN_NAME_FIELD,
                         ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD, ARTIFACT_VER_FIELD)
        .build();

    // Universal Plugin Data table
    public static final StructuredTableSpecification UNIV_PLUGIN_DATA_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(UNIV_PLUGIN_DATA_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(PLUGIN_TYPE_FIELD),
                    Fields.stringType(PLUGIN_NAME_FIELD),
                    Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                    Fields.stringType(ARTIFACT_NAME_FIELD),
                    Fields.stringType(ARTIFACT_VER_FIELD),
                    Fields.stringType(PLUGIN_DATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, PLUGIN_TYPE_FIELD, PLUGIN_NAME_FIELD,
                         ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD, ARTIFACT_VER_FIELD)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin,
                                    boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(ARTIFACT_DATA_TABLE) == null) {
        tableAdmin.create(ARTIFACT_DATA_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(APP_DATA_TABLE) == null) {
        tableAdmin.create(APP_DATA_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(PLUGIN_DATA_TABLE) == null) {
        tableAdmin.create(PLUGIN_DATA_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(UNIV_PLUGIN_DATA_TABLE) == null) {
        tableAdmin.create(UNIV_PLUGIN_DATA_SPEC);
      }
    }
  }

  /**
   * Table specification and create table definitions for owner store.
   */
  public static final class OwnerStore {
    public static final StructuredTableId OWNER_TABLE = new StructuredTableId("owner_data");
    public static final String PRINCIPAL_FIELD = "principal";
    public static final String KEYTAB_FIELD = "keytab";

    public static final StructuredTableSpecification OWNER_TABLE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(OWNER_TABLE)
        .withFields(Fields.stringType(PRINCIPAL_FIELD),
                    Fields.bytesType(KEYTAB_FIELD))
        .withPrimaryKeys(PRINCIPAL_FIELD).build();

    public static void createTables(StructuredTableAdmin tableAdmin,
                                    boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(OWNER_TABLE) == null) {
        tableAdmin.create(OWNER_TABLE_SPEC);
      }
    }
  }

  /**
   * Schema for {@link SecretStore}.
   */
  public static final class SecretStore {

    public static final StructuredTableId SECRET_STORE_TABLE = new StructuredTableId("secret_store");
    public static final String NAMESPACE_FIELD = "namespace";
    public static final String SECRET_NAME_FIELD = "secret_name";
    public static final String SECRET_DATA_FIELD = "secret_data";

    public static final StructuredTableSpecification SECRET_STORE_SPEC = new StructuredTableSpecification.Builder()
      .withId(SECRET_STORE_TABLE)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(SECRET_NAME_FIELD),
                  Fields.bytesType(SECRET_DATA_FIELD))
      .withPrimaryKeys(NAMESPACE_FIELD, SECRET_NAME_FIELD)
      .build();

    public static void createTable(StructuredTableAdmin tableAdmin,
                                   boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(SECRET_STORE_TABLE) == null) {
        tableAdmin.create(SECRET_STORE_SPEC);
      }
    }
  }

  /**
   * Table specification and create table definitions for provisioner store.
   */
  public static final class ProvisionerStore {
    public static final StructuredTableId PROVISIONER_TABLE = new StructuredTableId("provisioner_data");
    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String PROGRAM_TYPE_FIELD = "program_type";
    public static final String PROGRAM_FIELD = "program";
    public static final String RUN_FIELD = "run";
    public static final String KEY_TYPE = "type";
    public static final String PROVISIONER_TASK_INFO_FIELD = "provisioner_task_info";

    public static final StructuredTableSpecification PROVISIONER_STORE_SPEC = new StructuredTableSpecification.Builder()
      .withId(PROVISIONER_TABLE)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(APPLICATION_FIELD),
                  Fields.stringType(VERSION_FIELD),
                  Fields.stringType(PROGRAM_TYPE_FIELD),
                  Fields.stringType(PROGRAM_FIELD),
                  Fields.stringType(RUN_FIELD),
                  Fields.stringType(KEY_TYPE),
                  Fields.stringType(PROVISIONER_TASK_INFO_FIELD))
      .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD,
                       PROGRAM_TYPE_FIELD, PROGRAM_FIELD, RUN_FIELD, KEY_TYPE)
      .build();

    public static void createTable(StructuredTableAdmin tableAdmin,
                                   boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(PROVISIONER_TABLE) == null) {
        tableAdmin.create(PROVISIONER_STORE_SPEC);
      }
    }
  }
  /**
   *  Defines schema for AppMetadata tables
   */
  public static final class AppMetadataStore {

    public static final StructuredTableId APPLICATION_SPECIFICATIONS = new StructuredTableId("application_specs");
    public static final StructuredTableId WORKFLOW_NODE_STATES = new StructuredTableId("workflow_node_states");
    public static final StructuredTableId RUN_RECORDS = new StructuredTableId("run_records");
    public static final StructuredTableId WORKFLOWS = new StructuredTableId("workflows");
    public static final StructuredTableId PROGRAM_COUNTS = new StructuredTableId("program_counts");
    // TODO: CDAP-14876 Move this table into it's own store, along with associated methods
    public static final StructuredTableId SUBSCRIBER_STATES = new StructuredTableId("subscriber_state");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String APPLICATION_DATA_FIELD = "application_data";
    public static final String PROGRAM_TYPE_FIELD = "program_type";
    public static final String PROGRAM_FIELD = "program";
    public static final String RUN_FIELD = "run";
    public static final String NODE_ID = "node_id";
    public static final String NODE_STATE_DATA = "node_state_data";
    public static final String RUN_STATUS = "run_status";
    public static final String RUN_START_TIME = "run_start_time";
    public static final String RUN_RECORD_DATA = "run_record_data";
    public static final String WORKFLOW_DATA = "workflow_data";
    public static final String COUNT_TYPE = "count_type";
    public static final String COUNTS = "counts";
    public static final String SUBSCRIBER_TOPIC = "subscriber_topic";
    public static final String SUBSCRIBER_MESSAGE = "subscriber_message";
    public static final String SUBSCRIBER = "subscriber";


    public static final StructuredTableSpecification APPLICATION_SPECIFICATIONS_TABLE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(APPLICATION_SPECIFICATIONS)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(VERSION_FIELD),
                    Fields.stringType(APPLICATION_DATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD)
        .build();

    public static final StructuredTableSpecification WORKFLOW_NODE_STATES_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(WORKFLOW_NODE_STATES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(VERSION_FIELD),
                    Fields.stringType(PROGRAM_TYPE_FIELD),
                    Fields.stringType(PROGRAM_FIELD),
                    Fields.stringType(RUN_FIELD),
                    Fields.stringType(NODE_ID),
                    Fields.stringType(NODE_STATE_DATA))
        .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_TYPE_FIELD, PROGRAM_FIELD,
                         RUN_FIELD, NODE_ID)
        .build();

    public static final StructuredTableSpecification RUN_RECORDS_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(RUN_RECORDS)
        .withFields(Fields.stringType(RUN_STATUS),
                    Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(VERSION_FIELD),
                    Fields.stringType(PROGRAM_TYPE_FIELD),
                    Fields.stringType(PROGRAM_FIELD),
                    Fields.longType(RUN_START_TIME),
                    Fields.stringType(RUN_FIELD),
                    Fields.stringType(RUN_RECORD_DATA))
        .withPrimaryKeys(RUN_STATUS, NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_TYPE_FIELD,
                         PROGRAM_FIELD, RUN_START_TIME, RUN_FIELD)
        .build();

    public static final StructuredTableSpecification WORKFLOWS_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(WORKFLOWS)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(VERSION_FIELD),
                    Fields.stringType(PROGRAM_TYPE_FIELD),
                    Fields.stringType(PROGRAM_FIELD),
                    Fields.stringType(RUN_FIELD),
                    Fields.stringType(WORKFLOW_DATA))
        .withPrimaryKeys(
          NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_TYPE_FIELD, PROGRAM_FIELD, RUN_FIELD)
        .build();

    public static final StructuredTableSpecification PROGRAM_COUNTS_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(PROGRAM_COUNTS)
        .withFields(Fields.stringType(COUNT_TYPE),
                    Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(VERSION_FIELD),
                    Fields.stringType(PROGRAM_TYPE_FIELD),
                    Fields.stringType(PROGRAM_FIELD),
                    Fields.longType(COUNTS))
        .withPrimaryKeys(
          COUNT_TYPE, NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_TYPE_FIELD, PROGRAM_FIELD)
        .build();

    public static final StructuredTableSpecification SUBSCRIBER_STATE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(SUBSCRIBER_STATES)
        .withFields(Fields.stringType(SUBSCRIBER_TOPIC),
                    Fields.stringType(SUBSCRIBER),
                    Fields.stringType(SUBSCRIBER_MESSAGE))
        .withPrimaryKeys(SUBSCRIBER_TOPIC, SUBSCRIBER)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin,
                                    boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(APPLICATION_SPECIFICATIONS) == null) {
        tableAdmin.create(APPLICATION_SPECIFICATIONS_TABLE_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(WORKFLOW_NODE_STATES) == null) {
        tableAdmin.create(WORKFLOW_NODE_STATES_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(RUN_RECORDS) == null) {
        tableAdmin.create(RUN_RECORDS_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(WORKFLOWS) == null) {
        tableAdmin.create(WORKFLOWS_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(PROGRAM_COUNTS) == null) {
        tableAdmin.create(PROGRAM_COUNTS_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(SUBSCRIBER_STATES) == null) {
        tableAdmin.create(SUBSCRIBER_STATE_SPEC);
      }
    }
  }

  /**
   * Dataset instance store schema
   */
  public static final class DatasetInstanceStore {

    public static final StructuredTableId DATASET_INSTANCES =
      new StructuredTableId("dataset_instances");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String DATASET_FIELD = "dataset";
    public static final String DATASET_METADATA_FIELD = "dataset_metadata";

    public static final StructuredTableSpecification DATASET_INSTANCES_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(DATASET_INSTANCES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(DATASET_FIELD),
                    Fields.stringType(DATASET_METADATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, DATASET_FIELD)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin,
                                    boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(DATASET_INSTANCES) == null) {
        tableAdmin.create(DATASET_INSTANCES_SPEC);
      }
    }
  }

  /**
   * Table schema for profile store.
   */
  public static final class ProfileStore {

    public static final StructuredTableId PROFILE_STORE_TABLE =
      new StructuredTableId("profile_store");
    public static final StructuredTableId PROFILE_ENTITY_STORE_TABLE =
      new StructuredTableId("profile_entity_store");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String PROFILE_ID_FIELD = "profile_id";
    public static final String ENTITY_ID_FIELD = "entity_id";
    public static final String PROFILE_DATA_FIELD = "profile_data";
    public static final String ENTITY_DATA_FIELD = "entity_data";

    public static final StructuredTableSpecification PROFILE_STORE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(PROFILE_STORE_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(PROFILE_ID_FIELD),
                    Fields.stringType(PROFILE_DATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, PROFILE_ID_FIELD)
        .build();

    public static final StructuredTableSpecification PROFILE_ENTITY_STORE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(PROFILE_ENTITY_STORE_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(PROFILE_ID_FIELD),
                    Fields.stringType(ENTITY_ID_FIELD),
                    Fields.stringType(ENTITY_DATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, PROFILE_ID_FIELD, ENTITY_ID_FIELD)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin,
                                    boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(PROFILE_STORE_TABLE) == null) {
        tableAdmin.create(PROFILE_STORE_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(PROFILE_ENTITY_STORE_TABLE) == null) {
        tableAdmin.create(PROFILE_ENTITY_STORE_SPEC);
      }
    }
  }

  /**
   * Table schema for program schedule store.
   */
  public static final class ProgramScheduleStore {

    public static final StructuredTableId PROGRAM_SCHEDULE_TABLE =
      new StructuredTableId("program_schedule_store");
    public static final StructuredTableId PROGRAM_TRIGGER_TABLE =
      new StructuredTableId("program_trigger_store");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String SCHEDULE_NAME = "schedule_name";
    public static final String SEQUENCE_ID = "sequence_id";
    public static final String SCHEDULE = "schedule";
    public static final String UPDATE_TIME = "update_time";
    public static final String STATUS = "status";
    public static final String TRIGGER_KEY = "trigger_key";


    public static final StructuredTableSpecification PROGRAM_SCHEDULE_STORE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(PROGRAM_SCHEDULE_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(VERSION_FIELD),
                    Fields.stringType(SCHEDULE_NAME),
                    Fields.stringType(SCHEDULE),
                    Fields.longType(UPDATE_TIME),
                    Fields.stringType(STATUS))
        .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, SCHEDULE_NAME)
        .build();

    public static final StructuredTableSpecification PROGRAM_TRIGGER_STORE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(PROGRAM_TRIGGER_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(VERSION_FIELD),
                    Fields.stringType(SCHEDULE_NAME),
                    Fields.intType(SEQUENCE_ID),
                    Fields.stringType(TRIGGER_KEY))
        .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, SCHEDULE_NAME, SEQUENCE_ID)
        .withIndexes(TRIGGER_KEY)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin,
                                    boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(PROGRAM_SCHEDULE_TABLE) == null) {
        tableAdmin.create(PROGRAM_SCHEDULE_STORE_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(PROGRAM_TRIGGER_TABLE) == null) {
        tableAdmin.create(PROGRAM_TRIGGER_STORE_SPEC);
      }
    }
  }

  /**
   * Dataset type store schema
   */
  public static final class DatasetTypeStore {

    public static final StructuredTableId DATASET_TYPES = new StructuredTableId("dataset_types");
    public static final StructuredTableId MODULE_TYPES = new StructuredTableId("module_types");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String MODULE_NAME_FIELD = "module_name";
    public static final String TYPE_NAME_FIELD = "type_name";
    public static final String DATASET_METADATA_FIELD = "dataset_metadata";

    public static final StructuredTableSpecification DATASET_TYPES_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(DATASET_TYPES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(TYPE_NAME_FIELD),
                    Fields.stringType(DATASET_METADATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, TYPE_NAME_FIELD)
        .build();
    public static final StructuredTableSpecification MODULE_TYPES_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(MODULE_TYPES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(MODULE_NAME_FIELD),
                    Fields.stringType(DATASET_METADATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, MODULE_NAME_FIELD)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin,
                                    boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(DATASET_TYPES) == null) {
        tableAdmin.create(DATASET_TYPES_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(MODULE_TYPES) == null) {
        tableAdmin.create(MODULE_TYPES_SPEC);
      }
    }
  }

  /**
   * Schema for lineage table.
   */
  public static final class LineageStore {

    public static final StructuredTableId DATASET_LINEAGE_TABLE = new StructuredTableId("dataset_lineage");
    public static final StructuredTableId PROGRAM_LINEAGE_TABLE = new StructuredTableId("program_lineage");
    public static final String NAMESPACE_FIELD = "namespace";
    public static final String DATASET_FIELD = "dataset";
    public static final String START_TIME_FIELD = "start_time";
    public static final String PROGRAM_NAMESPACE_FIELD = "program_namespace";
    public static final String PROGRAM_APPLICATION_FIELD = "program_application";
    public static final String PROGRAM_TYPE_FIELD = "program_type";
    public static final String PROGRAM_FIELD = "program";
    public static final String RUN_FIELD = "run";
    public static final String ACCESS_TYPE_FIELD = "access_type";
    public static final String ACCESS_TIME_FIELD = "access_time";

    public static final StructuredTableSpecification DATASET_LINEAGE_SPEC = new StructuredTableSpecification.Builder()
      .withId(DATASET_LINEAGE_TABLE)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(DATASET_FIELD),
                  Fields.longType(START_TIME_FIELD),
                  Fields.stringType(PROGRAM_NAMESPACE_FIELD),
                  Fields.stringType(PROGRAM_APPLICATION_FIELD),
                  Fields.stringType(PROGRAM_TYPE_FIELD),
                  Fields.stringType(PROGRAM_FIELD),
                  Fields.stringType(RUN_FIELD),
                  Fields.stringType(ACCESS_TYPE_FIELD),
                  Fields.longType(ACCESS_TIME_FIELD))
      .withPrimaryKeys(NAMESPACE_FIELD, DATASET_FIELD, START_TIME_FIELD, PROGRAM_NAMESPACE_FIELD,
                       PROGRAM_APPLICATION_FIELD,
                       PROGRAM_TYPE_FIELD, PROGRAM_FIELD, RUN_FIELD, ACCESS_TYPE_FIELD)
      .build();

    public static final StructuredTableSpecification PROGRAM_LINEAGE_SPEC = new StructuredTableSpecification.Builder()
      .withId(PROGRAM_LINEAGE_TABLE)
      .withFields(Fields.stringType(PROGRAM_NAMESPACE_FIELD),
                  Fields.stringType(PROGRAM_APPLICATION_FIELD),
                  Fields.stringType(PROGRAM_TYPE_FIELD),
                  Fields.stringType(PROGRAM_FIELD),
                  Fields.longType(START_TIME_FIELD),
                  Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(DATASET_FIELD),
                  Fields.stringType(RUN_FIELD),
                  Fields.stringType(ACCESS_TYPE_FIELD),
                  Fields.longType(ACCESS_TIME_FIELD))
      .withPrimaryKeys(PROGRAM_NAMESPACE_FIELD, PROGRAM_APPLICATION_FIELD, PROGRAM_TYPE_FIELD, PROGRAM_FIELD,
                       START_TIME_FIELD, NAMESPACE_FIELD, DATASET_FIELD, RUN_FIELD, ACCESS_TYPE_FIELD)
      .build();

    public static void createTable(StructuredTableAdmin tableAdmin,
                                   boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(DATASET_LINEAGE_TABLE) == null) {
        tableAdmin.create(DATASET_LINEAGE_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(PROGRAM_LINEAGE_TABLE) == null) {
        tableAdmin.create(PROGRAM_LINEAGE_SPEC);
      }
    }
  }

  /**
   * Table schema for job queue.
   */
  public static final class JobQueueStore {

    public static final StructuredTableId JOB_QUEUE_TABLE =
      new StructuredTableId("job_queue_store");

    public static final String PARTITION_ID = "partition_id";
    public static final String SCHEDULE_ID = "schedule_id";
    public static final String GENERATION_ID = "generation_id";
    public static final String ROW_TYPE = "row_type";
    public static final String JOB = "job";
    public static final String DELETE_TIME = "delete_time";
    public static final String OBSOLETE_TIME = "obsolete_time";

    /**
     * Specifies the type of the data in a row. This is used as part of the primary key
     */
    public enum RowType {
      JOB, // row contains the serialized job
      DELETE, // if the job is marked for deletion, the row contains the time when the job was marked for deletion
      OBSOLETE // if the job has timed out, the row contains the time when the job was marked as obsolete
    }

    public static final StructuredTableSpecification JOB_QUEUE_STORE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(JOB_QUEUE_TABLE)
        .withFields(Fields.intType(PARTITION_ID),
                    Fields.stringType(SCHEDULE_ID),
                    Fields.intType(GENERATION_ID),
                    Fields.stringType(ROW_TYPE),
                    Fields.stringType(JOB),
                    Fields.longType(DELETE_TIME),
                    Fields.longType(OBSOLETE_TIME))
        .withPrimaryKeys(PARTITION_ID, SCHEDULE_ID, GENERATION_ID, ROW_TYPE)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin,
                                    boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(JOB_QUEUE_TABLE) == null) {
        tableAdmin.create(JOB_QUEUE_STORE_SPEC);
      }
    }
  }

  /**
   * Schema for time schedules.
   */
  public static final class TimeScheduleStore {

    public static final StructuredTableId SCHEDULES = new StructuredTableId("schedules");

    public static final String TYPE_FIELD = "type";
    public static final String NAME_FIELD = "name";
    public static final String VALUE_FIELD = "value";

    public static final StructuredTableSpecification SCHEDULES_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(SCHEDULES)
        .withFields(Fields.stringType(TYPE_FIELD),
                    Fields.stringType(NAME_FIELD),
                    Fields.bytesType(VALUE_FIELD))
        .withPrimaryKeys(TYPE_FIELD, NAME_FIELD)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin,
                                    boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(SCHEDULES) == null) {
        tableAdmin.create(SCHEDULES_SPEC);
      }
    }
  }

  /**
   * Schema for remote runtime
   */
  public static final class RemoteRuntimeStore {
    public static final StructuredTableId RUNTIMES = new StructuredTableId("runtimes");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String PROGRAM_TYPE_FIELD = "program_type";
    public static final String PROGRAM_FIELD = "program";
    public static final String RUN_FIELD = "run";
    public static final String PROGRAM_OPTIONS_FIELD = "program_options";

    public static final StructuredTableSpecification RUNTIMES_SPEC = new StructuredTableSpecification.Builder()
      .withId(RUNTIMES)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(APPLICATION_FIELD),
                  Fields.stringType(VERSION_FIELD),
                  Fields.stringType(PROGRAM_TYPE_FIELD),
                  Fields.stringType(PROGRAM_FIELD),
                  Fields.stringType(RUN_FIELD),
                  Fields.stringType(PROGRAM_OPTIONS_FIELD))
      .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_TYPE_FIELD, PROGRAM_FIELD, RUN_FIELD)
      .build();

    public static void createTables(StructuredTableAdmin tableAdmin,
                                    boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(RUNTIMES) == null) {
        tableAdmin.create(RUNTIMES_SPEC);
      }
    }
  }

  /**
   * Schema for program heartbeat.
   */
  public static final class ProgramHeartbeatStore {
    public static final StructuredTableId PROGRAM_HEARTBEATS = new StructuredTableId("program_heartbeats");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String TIMESTAMP_SECONDS_FIELD = "timestamp";
    public static final String APPLICATION_FIELD = "application";
    public static final String PROGRAM_TYPE_FIELD = "program_type";
    public static final String PROGRAM_FIELD = "program";
    public static final String RUN_FIELD = "run";
    public static final String RUN_RECORD = "run_record";

    public static final StructuredTableSpecification PROGRAM_HEARTBEATS_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(PROGRAM_HEARTBEATS)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.longType(TIMESTAMP_SECONDS_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(PROGRAM_TYPE_FIELD),
                    Fields.stringType(PROGRAM_FIELD),
                    Fields.stringType(RUN_FIELD),
                    Fields.stringType(RUN_RECORD))
        .withPrimaryKeys(
          NAMESPACE_FIELD, TIMESTAMP_SECONDS_FIELD, APPLICATION_FIELD, PROGRAM_TYPE_FIELD, PROGRAM_FIELD, RUN_FIELD)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin
      , boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(ProgramHeartbeatStore.PROGRAM_HEARTBEATS) == null) {
        tableAdmin.create(PROGRAM_HEARTBEATS_SPEC);
      }
    }
  }

  /**
   * Schema for log checkpoint store.
   */
  public static final class LogCheckpointStore {

    public static final StructuredTableId LOG_CHECKPOINT_TABLE = new StructuredTableId("log_checkpoints");
    public static final String ROW_PREFIX_FIELD = "prefix";
    public static final String PARTITION_FIELD = "partition";
    public static final String CHECKPOINT_FIELD = "checkpoint";

    public static final StructuredTableSpecification LOG_CHECKPOINT_TABLE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(LOG_CHECKPOINT_TABLE)
        .withFields(Fields.stringType(ROW_PREFIX_FIELD),
                    Fields.intType(PARTITION_FIELD),
                    Fields.bytesType(CHECKPOINT_FIELD))
        .withPrimaryKeys(ROW_PREFIX_FIELD, PARTITION_FIELD)
        .build();

    public static void createTable(StructuredTableAdmin tableAdmin,
                                   boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(LOG_CHECKPOINT_TABLE) == null) {
        tableAdmin.create(LOG_CHECKPOINT_TABLE_SPEC);
      }
    }
  }

  /**
   *  Schema for usage table
   */
  public static final class UsageStore {
    public static final StructuredTableId USAGES = new StructuredTableId("usages");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String PROGRAM_TYPE_FIELD = "program_type";
    public static final String PROGRAM_FIELD = "program";
    public static final String DATASET_FIELD = "dataset";
    public static final String INDEX_FIELD = "index";

    public static final StructuredTableSpecification USAGES_SPEC = new StructuredTableSpecification.Builder()
      .withId(USAGES)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(APPLICATION_FIELD),
                  Fields.stringType(PROGRAM_TYPE_FIELD),
                  Fields.stringType(PROGRAM_FIELD),
                  Fields.stringType(DATASET_FIELD),
                  Fields.stringType(INDEX_FIELD))
      .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, PROGRAM_TYPE_FIELD, PROGRAM_FIELD, DATASET_FIELD)
      .withIndexes(INDEX_FIELD)
      .build();

    public static void createTables(StructuredTableAdmin tableAdmin,
                                    boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(USAGES) == null) {
        tableAdmin.create(USAGES_SPEC);
      }
    }
  }

  /**
   * Schema for field lineage.
   *
   * Endpoint checksum table is used to store endpoints/properties of endpoints to a checksum. Checksum can then be
   * used the query the other tables. Also contains the program run info for that checksum.
   *
   * The remaining tables store various endpoint data keyed by checksum.
   */
  public static final class FieldLineageStore {

    public static final StructuredTableId ENDPOINT_CHECKSUM_TABLE = new StructuredTableId("fields_table");
    public static final StructuredTableId OPERATIONS_TABLE = new StructuredTableId("operations_table");
    public static final StructuredTableId DESTINATION_FIELDS_TABLE = new StructuredTableId("destination_fields_table");
    public static final StructuredTableId SUMMARY_FIELDS_TABLE = new StructuredTableId("summary_fields_table");

    public static final String DIRECTION_FIELD = "direction";
    public static final String ENDPOINT_NAMESPACE_FIELD = "endpoint_namespace";
    public static final String ENDPOINT_NAME_FIELD = "endpoint";
    public static final String START_TIME_FIELD = "start_time";
    public static final String CHECKSUM_FIELD = "checksum";
    public static final String PROGRAM_RUN_FIELD = "program_run";
    public static final String OPERATIONS_FIELD = "operations";
    public static final String DESTINATION_DATA_FIELD = "destination_data";
    public static final String ENDPOINT_FIELD = "endpoint_field";

    public static final StructuredTableSpecification ENDPOINT_CHECKSUM_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(ENDPOINT_CHECKSUM_TABLE)
        .withFields(Fields.stringType(DIRECTION_FIELD),
                    Fields.stringType(ENDPOINT_NAMESPACE_FIELD),
                    Fields.stringType(ENDPOINT_NAME_FIELD),
                    Fields.longType(START_TIME_FIELD),
                    Fields.longType(CHECKSUM_FIELD),
                    Fields.stringType(PROGRAM_RUN_FIELD))
        .withPrimaryKeys(DIRECTION_FIELD, ENDPOINT_NAMESPACE_FIELD, ENDPOINT_NAME_FIELD, START_TIME_FIELD)
        .build();
    public static final StructuredTableSpecification OPERATIONS_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(OPERATIONS_TABLE)
        .withFields(Fields.longType(CHECKSUM_FIELD),
                    Fields.stringType(OPERATIONS_FIELD))
        .withPrimaryKeys(CHECKSUM_FIELD)
        .build();
    public static final StructuredTableSpecification DESTINATION_FIELDS_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(DESTINATION_FIELDS_TABLE)
        .withFields(Fields.longType(CHECKSUM_FIELD),
                    Fields.stringType(ENDPOINT_NAMESPACE_FIELD),
                    Fields.stringType(ENDPOINT_NAME_FIELD),
                    Fields.stringType(DESTINATION_DATA_FIELD))
        .withPrimaryKeys(CHECKSUM_FIELD, ENDPOINT_NAMESPACE_FIELD, ENDPOINT_NAME_FIELD)
        .build();
    public static final StructuredTableSpecification SUMMARY_FIELDS_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(SUMMARY_FIELDS_TABLE)
        .withFields(Fields.longType(CHECKSUM_FIELD),
                    Fields.stringType(DIRECTION_FIELD),
                    Fields.stringType(ENDPOINT_NAMESPACE_FIELD),
                    Fields.stringType(ENDPOINT_NAME_FIELD),
                    Fields.stringType(ENDPOINT_FIELD),
                    Fields.stringType(DESTINATION_DATA_FIELD))
        .withPrimaryKeys(CHECKSUM_FIELD, DIRECTION_FIELD, ENDPOINT_NAMESPACE_FIELD, ENDPOINT_NAME_FIELD,
                         ENDPOINT_FIELD)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin,
                                    boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(ENDPOINT_CHECKSUM_TABLE) == null) {
        tableAdmin.create(ENDPOINT_CHECKSUM_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(OPERATIONS_TABLE) == null) {
        tableAdmin.create(OPERATIONS_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(DESTINATION_FIELDS_TABLE) == null) {
        tableAdmin.create(DESTINATION_FIELDS_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(SUMMARY_FIELDS_TABLE) == null) {
        tableAdmin.create(SUMMARY_FIELDS_SPEC);
      }
    }
  }

  /**
   * Schema for log file meta.
   */
  public static final class LogFileMetaStore {
    public static final StructuredTableId LOG_FILE_META = new StructuredTableId("logfile_meta");

    public static final String LOGGING_CONTEXT_FIELD = "logging_context";
    public static final String EVENT_TIME_FIELD = "event_time";
    public static final String CREATION_TIME_FIELD = "creation_time";
    public static final String FILE_FIELD = "file";

    public static final StructuredTableSpecification LOG_FILE_META_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(LOG_FILE_META)
        .withFields(Fields.stringType(LOGGING_CONTEXT_FIELD),
                    Fields.longType(EVENT_TIME_FIELD),
                    Fields.longType(CREATION_TIME_FIELD),
                    Fields.stringType(FILE_FIELD))
        .withPrimaryKeys(LOGGING_CONTEXT_FIELD, EVENT_TIME_FIELD, CREATION_TIME_FIELD).build();

    public static void createTables(StructuredTableAdmin tableAdmin,
                                    boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(LOG_FILE_META) == null) {
        tableAdmin.create(LOG_FILE_META_SPEC);
      }
    }
  }

  /**
   * Schema for Capabilities
   */
  public static final class CapabilitiesStore {
    public static final StructuredTableId CAPABILITIES = new StructuredTableId("capabilities");
    public static final StructuredTableId CAPABILITY_OPERATIONS = new StructuredTableId("capability_operations");

    public static final String NAME_FIELD = "name";
    public static final String STATUS_FIELD = "status";
    public static final String ACTION_FIELD = "action";
    public static final String CONFIG_FIELD = "config";
    public static final String UPDATED_TIME_FIELD = "updated_time";

    public static final StructuredTableSpecification CAPABILITIES_TABLE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(CAPABILITIES)
        .withFields(Fields.stringType(NAME_FIELD),
                    Fields.stringType(STATUS_FIELD),
                    Fields.stringType(CONFIG_FIELD),
                    Fields.longType(UPDATED_TIME_FIELD))
        .withPrimaryKeys(NAME_FIELD)
        .build();

    public static final StructuredTableSpecification CAPABILITY_OPERATIONS_TABLE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(CAPABILITY_OPERATIONS)
        .withFields(Fields.stringType(NAME_FIELD),
                    Fields.stringType(ACTION_FIELD),
                    Fields.stringType(CONFIG_FIELD))
        .withPrimaryKeys(NAME_FIELD)
        .build();

    public static void createTable(StructuredTableAdmin tableAdmin,
                                   boolean overWrite) throws IOException, TableAlreadyExistsException {
      if (overWrite || tableAdmin.getSpecification(CAPABILITIES) == null) {
        tableAdmin.create(CAPABILITIES_TABLE_SPEC);
      }
      if (overWrite || tableAdmin.getSpecification(CAPABILITY_OPERATIONS) == null) {
        tableAdmin.create(CAPABILITY_OPERATIONS_TABLE_SPEC);
      }
    }
  }
}
