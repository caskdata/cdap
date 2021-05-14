/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

package io.cdap.cdap.common.conf;

import com.google.common.collect.ImmutableMap;
import com.google.inject.BindingAnnotation;
import io.cdap.cdap.proto.id.NamespaceId;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;

/**
 * Constants used by different systems are all defined here.
 */
public final class Constants {

  public static final String[] FEATURE_TOGGLE_PROPS = {
    Security.SSL.EXTERNAL_ENABLED,
    Security.SSL.INTERNAL_ENABLED,
    Security.ENABLED,
    Explore.EXPLORE_ENABLED,
  };

  public static final String[] PORT_PROPS = {
    Router.ROUTER_PORT,
    Router.ROUTER_SSL_PORT,
    Dashboard.BIND_PORT,
    Dashboard.SSL_BIND_PORT,
    Security.AUTH_SERVER_BIND_PORT,
    Security.AuthenticationServer.SSL_PORT,
  };

  public static final String ARCHIVE_DIR = "archive";
  public static final String ROOT_NAMESPACE = "root.namespace";
  public static final String COLLECT_CONTAINER_LOGS = "master.collect.containers.log";
  public static final String COLLECT_APP_CONTAINER_LOG_LEVEL = "master.collect.app.containers.log.level";
  public static final String HTTP_CLIENT_CONNECTION_TIMEOUT_MS = "http.client.connection.timeout.ms";
  public static final String HTTP_CLIENT_READ_TIMEOUT_MS = "http.client.read.timeout.ms";
  /** Uniquely identifies a CDAP instance */
  public static final String INSTANCE_NAME = "instance.name";
  // Environment variable name for spark home
  public static final String SPARK_HOME = "SPARK_HOME";
  // Environment variable name for spark compat version
  public static final String SPARK_COMPAT_ENV = "SPARK_COMPAT";
  // File specifying bootstrap steps
  public static final String BOOTSTRAP_FILE = "bootstrap.file";
  // Directory path location for all system app configs
  public static final String SYSTEM_APP_CONFIG_DIR = "system.app.config.dir";
  // Directory path for caching remove location content
  public static final String LOCATION_CACHE_PATH = "location.cache.path";
  public static final String LOCATION_CACHE_EXPIRATION_MS = "location.cache.expiration.ms";

  public static final String CLUSTER_NAME = "cluster.name";
  /* Used by the user to specify what part of a path should be replaced by the current user's name. */
  public static final String USER_NAME_SPECIFIER = "${name}";

  /* Used to specify a comma separated list of plugin dataset type requirements that cannot be met in the instance.
  Plugins that require any of these will be treated as if they don't exist.
  For example, if 'table' is given, any plugin that requires 'table' will be treated as
  if they don't exist. */
  public static final String REQUIREMENTS_DATASET_TYPE_EXCLUDE = "requirements.datasetTypes.exclude.list";


  // Configurations for external proxy support for out going connections.
  // Configuration key for the monitor proxy in the format of "host:port"
  public static final String NETWORK_PROXY_ADDRESS = "network.proxy.address";

  /**
   * Configuration for Master startup.
   */
  public static final class Startup {
    public static final String CHECKS_ENABLED = "master.startup.checks.enabled";
    public static final String CHECK_PACKAGES = "master.startup.checks.packages";
    public static final String CHECK_CLASSES = "master.startup.checks.classes";
    public static final String YARN_CONNECT_TIMEOUT_SECONDS = "master.startup.checks.yarn.connect.timeout.seconds";
    public static final String STARTUP_SERVICE_TIMEOUT = "master.startup.service.timeout.seconds";
    /* Used by transaction pruning to determine if cdap is global admin or not for hbase version 0.96 and 0.98 */
    public static final String TX_PRUNE_ACL_CHECK = "data.tx.prune.acl.check.override";
  }

  /**
   * Configuration for Master.
   */
  public static final class Master {
    public static final String EXTENSIONS_DIR = "master.environment.extensions.dir";
    public static final String MAX_INSTANCES = "master.service.max.instances";
  }

  /**
   * Global Service names.
   */
  public static final class Service {
    public static final String APP_FABRIC_HTTP = "appfabric";
    public static final String TRANSACTION = "transaction";
    public static final String TRANSACTION_HTTP = "transaction.http";
    public static final String METRICS = "metrics";
    public static final String LOGSAVER = "log.saver";
    public static final String LOG_QUERY = "log.query";
    public static final String GATEWAY = "gateway";
    public static final String MASTER_SERVICES = "master.services";
    public static final String METRICS_PROCESSOR = "metrics.processor";
    public static final String DATASET_MANAGER = "dataset.service";
    public static final String DATASET_EXECUTOR = "dataset.executor";
    public static final String EXTERNAL_AUTHENTICATION = "external.authentication";
    public static final String EXPLORE_HTTP_USER_SERVICE = "explore.service";
    public static final String MESSAGING_SERVICE = "messaging.service";
    public static final String RUNTIME = "runtime";

    public static final String SERVICE_INSTANCE_TABLE_NAME = "cdap.services.instances";
    /** Scheduler queue name to submit the master service app. */
    public static final String SCHEDULER_QUEUE = "master.services.scheduler.queue";
    public static final String METADATA_SERVICE = "metadata.service";
    public static final String MASTER_SERVICES_BIND_ADDRESS = "master.services.bind.address";
    public static final String MASTER_SERVICES_ANNOUNCE_ADDRESS = "master.services.announce.address";
    public static final String PREVIEW_HTTP = "preview";
    public static final String SECURE_STORE_SERVICE = "secure.store.service";
    public static final String LOG_BUFFER_SERVICE = "log.buffer.service";
  }

  /**
   * ZooKeeper Configuration.
   */
  public static final class Zookeeper {
    public static final String QUORUM = "zookeeper.quorum";
    public static final String CFG_SESSION_TIMEOUT_MILLIS = "zookeeper.session.timeout.millis";
    public static final String CLIENT_STARTUP_TIMEOUT_MILLIS = "zookeeper.client.startup.timeout.millis";
    public static final int DEFAULT_SESSION_TIMEOUT_MILLIS = 40000;

    // System property name to control the InMemoryZKServer to bind to localhost only or not
    public static final String TWILL_ZK_SERVER_LOCALHOST = "twill.zk.server.localhost";
  }

  /**
   * HBase configurations.
   */
  public static final class HBase {
    public static final String AUTH_KEY_UPDATE_INTERVAL = "hbase.auth.key.update.interval";
    public static final String MANAGE_COPROCESSORS = "master.manage.hbase.coprocessors";
    public static final String CLIENT_RETRIES = "hbase.client.retries.number";
    public static final String RPC_TIMEOUT = "hbase.rpc.timeout";
    /** Determines how to behave when the HBase version is unsupported. cdap_set_hbase() method
     * in cdap-common/bin/functions.sh must also be updated if this String is changed */
    public static final String HBASE_VERSION_RESOLUTION_STRATEGY = "hbase.version.resolution.strategy";
    /** Keep HBase version as it is when HBase version is unsupported. cdap_set_hbase() method
     * in cdap-common/bin/functions.sh must also be updated if this String is changed */
    public static final String HBASE_AUTO_STRICT_VERSION = "auto.strict";
    /** Use latest HBase version available on the cluster when HBase version is unsupported. cdap_set_hbase() method
     * in cdap-common/bin/functions.sh must also be updated if this String is changed */
    public static final String HBASE_AUTO_LATEST_VERSION = "auto.latest";
  }

  /**
   * Dangerous Options.
   */
  public static final class Dangerous {
    public static final String UNRECOVERABLE_RESET = "enable.unrecoverable.reset";
    public static final boolean DEFAULT_UNRECOVERABLE_RESET = false;
  }

  /**
   * App Fabric Configuration.
   */
  public static final class AppFabric {

    /**
     * App Fabric Server.
     */
    public static final String SERVER_PORT = "app.bind.port";
    public static final String SERVER_ANNOUNCE_PORT = "app.announce.port";
    public static final String OUTPUT_DIR = "app.output.dir";
    public static final String TEMP_DIR = "app.temp.dir";
    public static final String REST_PORT = "app.rest.port";
    public static final String PROGRAM_JVM_OPTS = "app.program.jvm.opts";
    public static final String BACKLOG_CONNECTIONS = "app.connection.backlog";
    public static final String EXEC_THREADS = "app.exec.threads";
    public static final String BOSS_THREADS = "app.boss.threads";
    public static final String WORKER_THREADS = "app.worker.threads";
    public static final String APP_SCHEDULER_QUEUE = "apps.scheduler.queue";
    public static final String STATUS_EVENT_FETCH_SIZE = "app.program.status.event.fetch.size";
    public static final String STATUS_EVENT_POLL_DELAY_MILLIS = "app.program.status.event.poll.delay.millis";
    public static final String MAPREDUCE_JOB_CLIENT_CONNECT_MAX_RETRIES = "mapreduce.jobclient.connect.max.retries";
    public static final String MAPREDUCE_INCLUDE_CUSTOM_CLASSES = "mapreduce.include.custom.format.classes";
    public static final String MAPREDUCE_STATUS_REPORT_INTERVAL_SECONDS = "mapreduce.status.report.interval.seconds";
    public static final String PROGRAM_RUNID_CORRECTOR_INTERVAL_SECONDS = "app.program.runid.corrector.interval";
    public static final String PROGRAM_RUNID_CORRECTOR_TX_BATCH_SIZE = "app.program.runid.corrector.tx.batch.size";
    public static final String LOCAL_DATASET_DELETER_INTERVAL_SECONDS = "app.program.local.dataset.deleter.interval";
    public static final String LOCAL_DATASET_DELETER_INITIAL_DELAY_SECONDS
      = "app.program.local.dataset.deleter.initial.delay";
    public static final String SYSTEM_ARTIFACTS_DIR = "app.artifact.dir";
    public static final String PROGRAM_EXTRA_CLASSPATH = "app.program.extra.classpath";
    public static final String SPARK_YARN_CLIENT_REWRITE = "app.program.spark.yarn.client.rewrite.enabled";
    public static final String SPARK_EVENT_LOGS_ENABLED = "app.program.spark.event.logs.enabled";
    public static final String SPARK_EVENT_LOGS_DIR = "app.program.spark.event.logs.dir";
    public static final String SPARK_COMPAT = "app.program.spark.compat";
    public static final String RUNTIME_EXT_DIR = "app.program.runtime.extensions.dir";
    public static final String PROGRAM_MAX_START_SECONDS = "app.program.max.start.seconds";
    public static final String PROGRAM_MAX_STOP_SECONDS = "app.program.max.stop.seconds";
    public static final String YARN_ATTEMPT_FAILURES_VALIDITY_INTERVAL =
      "app.program.yarn.attempt.failures.validity.interval";

    public static final String PROGRAM_TRANSACTION_CONTROL = "app.program.transaction.control";
    public static final String MAX_CONCURRENT_RUNS = "app.max.concurrent.runs";
    public static final String PROGRAM_LAUNCH_THREADS = "app.program.launch.threads";

    // A boolean value cConf entry to tell whether a ProgramRunner is running remotely (i.e. not inside app-fabric)
    // This config is not present in the cdap-default.xml as it is only set internally by CDAP.
    public static final String PROGRAM_REMOTE_RUNNER = "app.program.remote.runner";

    /**
     * Guice named bindings.
     */
    public static final String HANDLERS_BINDING = "appfabric.http.handler";

    /**
     * Defaults.
     */
    public static final int DEFAULT_BACKLOG = 20000;
    public static final int DEFAULT_EXEC_THREADS = 20;
    public static final int DEFAULT_BOSS_THREADS = 1;
    public static final int DEFAULT_WORKER_THREADS = 10;
    public static final boolean DEFAULT_APP_UPDATE_SCHEDULES = true;

    /**
     * Query parameter to indicate start time.
     */
    public static final String QUERY_PARAM_START_TIME = "start";

    /**
     * Query parameter to indicate status of a program {active, completed, failed}
     */
    public static final String QUERY_PARAM_STATUS = "status";

    /**
     * Query parameter to indicate end time.
     */
    public static final String QUERY_PARAM_END_TIME = "end";

    /**
     * Query parameter to indicate limits on results.
     */
    public static final String QUERY_PARAM_LIMIT = "limit";

    public static final String SERVICE_DESCRIPTION = "Service for managing application lifecycle.";

    /**
     * Configuration setting to set the maximum size of a workflow token in MB
     */
    public static final String WORKFLOW_TOKEN_MAX_SIZE_MB = "workflow.token.max.size.mb";

    /**
     * Name of the property used to identify whether the dataset is local or not.
     */
    public static final String WORKFLOW_LOCAL_DATASET_PROPERTY = "workflow.local.dataset";

    public static final String WORKFLOW_NAMESPACE_NAME = "workflow.namespace.name";

    public static final String WORKFLOW_APPLICATION_NAME = "workflow.application.name";

    public static final String WORKFLOW_APPLICATION_VERSION = "workflow.application.version";

    public static final String WORKFLOW_PROGRAM_NAME = "workflow.program.name";

    public static final String WORKFLOW_RUN_ID = "workflow.run.id";

    public static final String WORKFLOW_KEEP_LOCAL = "workflow.keep.local";

    /**
     * Configuration setting to localize extra jars to every program container and to be
     * added to classpaths of CDAP programs.
     */
    public static final String PROGRAM_CONTAINER_DIST_JARS = "program.container.dist.jars";

    public static final String APP_UPDATE_SCHEDULES = "app.deploy.update.schedules";

    /**
     * Topic name for publishing status transitioning events of program runs to the messaging system
     */
    public static final String PROGRAM_STATUS_EVENT_TOPIC = "program.status.event.topic";

    /**
     * Topic name for publishing program status recording events to the messaging system
     */
    public static final String PROGRAM_STATUS_RECORD_EVENT_TOPIC = "program.status.record.event.topic";

    /**
     * Interval at which system programs are monitored
     */
    public static final String SYSTEM_PROGRAM_SCAN_INTERVAL_SECONDS = "system.program.scan.interval.seconds";

    /**
     * Annotation for binding remote execution twill service
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @BindingAnnotation
    public @interface RemoteExecution { }

    /**
     * A special annotation used in Guice bindings for ProgramRunner implementations.
     * It is needed so that we can have different bindings in different private modules,
     * without affecting/affected by unannotated bindings in the public space.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @BindingAnnotation
    public @interface ProgramRunner { }
  }

  /**
   * Preview Configurations
   */
  public static final class Preview {
    public static final String ENABLED = "enable.preview";

    /**
     * Guice named bindings.
     */
    public static final String ADDRESS = "preview.bind.address";
    public static final String PORT = "preview.bind.port";
    public static final String BACKLOG_CONNECTIONS = "preview.connection.backlog";
    public static final String EXEC_THREADS = "preview.exec.threads";
    public static final String BOSS_THREADS = "preview.boss.threads";
    public static final String WORKER_THREADS = "preview.worker.threads";

    public static final String POLLER_COUNT = "preview.poller.count";
    public static final String REQUEST_POLL_DELAY_MILLIS = "preview.request.poll.delay.millis";
    public static final String MAX_RUNS = "preview.max.runs";
    public static final String WAITING_QUEUE_CAPACITY = "preview.waiting.queue.capacity";
    public static final String WAITING_QUEUE_TIMEOUT_SECONDS = "preview.waiting.queue.timeout.seconds";
    public static final String MESSAGING_TOPIC = "preview.messaging.topic";
    public static final String DATA_CLEANUP_INTERVAL_SECONDS = "preview.data.cleanup.interval.seconds";
    public static final String DATA_TTL_SECONDS = "preview.data.ttl.seconds";

    public static final String CONTAINER_COUNT = "preview.runner.container.count";
    public static final String CONTAINER_DISK_SIZE_GB = "preview.runner.container.disk.size.gb";
    public static final String CONTAINER_MEMORY_MB = "preview.runner.container.memory.mb";
    public static final String CONTAINER_CORES = "preview.runner.container.num.cores";

    public static final String CONTAINER_CPU_MULTIPLIER = "preview.runner.container.cpu.multiplier";
    public static final String CONTAINER_MEMORY_MULTIPLIER = "preview.runner.container.memory.multiplier";
    public static final String CONTAINER_HEAP_RESERVED_RATIO = "preview.runner.container.java.heap.memory.ratio";
    public static final String CONTAINER_PRIORITY_CLASS_NAME = "preview.runner.container.priority.class.name";
  }

  /**
   * Scheduler options.
   */
  public static final class Scheduler {
    public static final String CFG_SCHEDULER_MAX_THREAD_POOL_SIZE = "scheduler.max.thread.pool.size";
    public static final String CFG_SCHEDULER_MISFIRE_THRESHOLD_MS = "scheduler.misfire.threshold.ms";
    /**
     * Topic name for publishing time events from time scheduler to the messaging system
     */
    public static final String TIME_EVENT_TOPIC = "time.event.topic";

    public static final String EVENT_POLL_DELAY_MILLIS = "scheduler.event.poll.delay.millis";

    public static final String TIME_EVENT_FETCH_SIZE = "scheduler.time.event.fetch.size";
    public static final String DATA_EVENT_FETCH_SIZE = "scheduler.data.event.fetch.size";
    public static final String PROGRAM_STATUS_EVENT_FETCH_SIZE = "scheduler.program.status.event.fetch.size";

    public static final String JOB_QUEUE_NUM_PARTITIONS = "scheduler.job.queue.num.partitions";
  }

  /**
   * Application metadata store.
   */
  public static final class AppMetaStore {
    public static final String TABLE = "app.meta";
  }

  /**
   * Program heartbeat store.
   */
  public static final class ProgramHeartbeat {
    public static final String TABLE = "program.heartbeat";
    public static final String HEARTBEAT_INTERVAL_SECONDS = "program.heartbeat.interval.seconds";
    public static final String HEARTBEAT_TABLE_TTL_SECONDS = "program.heartbeat.table.ttl.seconds";
  }

  /**
   * Plugin Artifacts constants.
   */
  public static final class Plugin {
    // Key to be used in hConf to store location of the plugin artifact jar
    public static final String ARCHIVE = "cdap.program.plugin.archive";
  }

  /**
   * Configuration Store.
   */
  public static final class ConfigStore {
    public static final Byte VERSION = 0;
  }

  /**
   * Transactions.
   */
  public static final class Transaction {

    public static final String TX_ENABLED = "data.tx.enabled";

    /**
     * Twill Runnable configuration.
     */
    public static final class Container {
      // TODO: This is duplicated from TxConstants. Needs to be removed.
      public static final String ADDRESS = "data.tx.bind.address";
      public static final String NUM_INSTANCES = "data.tx.num.instances";
      public static final String NUM_CORES = "data.tx.num.cores";
      public static final String MEMORY_MB = "data.tx.memory.mb";
      public static final String MAX_INSTANCES = "data.tx.max.instances";
    }

    public static final String SERVICE_DESCRIPTION = "Service that maintains transaction states.";

    /**
     * Configuration for the TransactionDataJanitor coprocessor.
     */
    public static final class DataJanitor {
      /**
       * Whether or not the TransactionDataJanitor coprocessor should be enabled on tables.
       * Disable for testing.
       */
      public static final String CFG_TX_JANITOR_ENABLE = "data.tx.janitor.enable";
      public static final boolean DEFAULT_TX_JANITOR_ENABLE = true;
    }
  }

  /**
   * Datasets.
   */
  public static final class Dataset {

    public static final String TABLE_PREFIX = "dataset.table.prefix";

    // Table dataset property that defines whether table is transactional or not.
    // Currently it is hidden from user as only supported for specially treated Metrics System's HBase
    // tables. Constant could be moved to Table after that is changed. See CDAP-1193 for more info
    public static final String TABLE_TX_DISABLED = "dataset.table.tx.disabled";

    public static final String DATA_DIR = "dataset.data.dir";
    public static final String DEFAULT_DATA_DIR = "data";

    public static final String DATASET_UNCHECKED_UPGRADE = "dataset.unchecked.upgrade";

    public static final String DATA_EVENT_TOPIC = "data.event.topic";

    public static final String DATA_STORAGE_IMPLEMENTATION = "data.storage.implementation";
    public static final String DATA_STORAGE_NOSQL = "nosql";
    public static final String DATA_STORAGE_SQL = "postgresql";
    public static final String DATA_STORAGE_SQL_DRIVER_EXTERNAL = "data.storage.sql.jdbc.driver.external";
    public static final String DATA_STORAGE_SQL_DRIVER_DIRECTORY = "data.storage.sql.jdbc.driver.directory";
    public static final String DATA_STORAGE_SQL_JDBC_DRIVER_NAME = "data.storage.sql.jdbc.driver.name";

    // the jdbc connection related properties should be from cdap-security.xml
    public static final String DATA_STORAGE_SQL_JDBC_CONNECTION_URL = "data.storage.sql.jdbc.connection.url";
    public static final String DATA_STORAGE_SQL_USERNAME = "data.storage.sql.jdbc.username";
    public static final String DATA_STORAGE_SQL_PASSWORD = "data.storage.sql.jdbc.password";
    public static final String DATA_STORAGE_SQL_PROPERTY_PREFIX = "data.storage.sql.jdbc.property.";
    public static final String DATA_STORAGE_SQL_CONNECTION_SIZE = "data.storage.sql.jdbc.connection.pool.size";

    // used for Guice named bindings
    public static final String TABLE_TYPE = "table.type";
    public static final String TABLE_TYPE_NO_TX = "table.type.no.tx";

    /**
     * Constants for PartitionedFileSet's DynamicPartitioner
     */
    public static final class Partitioned {
      public static final String HCONF_ATTR_OUTPUT_DATASET = "output.dataset.name";
      public static final String HCONF_ATTR_OUTPUT_FORMAT_CLASS_NAME = "output.format.class.name";
    }

    /**
     * DatasetManager service configuration.
     */
    public static final class Manager {
      /** for the address (hostname) of the dataset server. */
      public static final String PORT = "dataset.service.bind.port";
      public static final String ANNOUNCE_PORT = "dataset.service.announce.port";

      public static final String BACKLOG_CONNECTIONS = "dataset.service.connection.backlog";
      public static final String EXEC_THREADS = "dataset.service.exec.threads";
      public static final String BOSS_THREADS = "dataset.service.boss.threads";
      public static final String WORKER_THREADS = "dataset.service.worker.threads";
      public static final String OUTPUT_DIR = "dataset.service.output.dir";

      /**
       * Annotation for binding default dataset modules for the dataset service
       */
      @Retention(RetentionPolicy.RUNTIME)
      @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
      @BindingAnnotation
      public @interface DefaultDatasetModules { }
    }

    /**
     * DatasetUserService configuration.
     */
    public static final class Executor {
      /** for the port of the dataset user service server. */
      public static final String PORT = "dataset.executor.bind.port";

      /** for the address (hostname) of the dataset server. */
      public static final String ADDRESS = "dataset.executor.bind.address";

      public static final String EXEC_THREADS = "dataset.executor.exec.threads";
      public static final String WORKER_THREADS = "dataset.executor.worker.threads";
      public static final String OUTPUT_DIR = "dataset.executor.output.dir";

      /** Twill Runnable configuration **/
      public static final String CONTAINER_VIRTUAL_CORES = "dataset.executor.container.num.cores";
      public static final String CONTAINER_MEMORY_MB = "dataset.executor.container.memory.mb";
      public static final String CONTAINER_INSTANCES = "dataset.executor.container.instances";

      //max-instances of dataset executor service
      public static final String MAX_INSTANCES = "dataset.executor.max.instances";

      public static final String SERVICE_DESCRIPTION = "Service to perform dataset operations.";
    }

    /**
     * Dataset extensions.
     */
    public static final class Extensions {
      public static final String MODULES = "dataset.extensions.modules";

      /** Over-rides for default table bindings- use with caution! **/
      public static final String DISTMODE_TABLE = "dataset.extensions.distributed.mode.table";
    }
  }

  /**
   * Gateway Configurations.
   */
  public static final class Gateway {

    /**
     * v3 API.
     */
    public static final String API_VERSION_3_TOKEN = "v3";
    public static final String API_VERSION_3 = "/" + API_VERSION_3_TOKEN;
    public static final String API_KEY = "X-ApiKey";

    /**
     * Internal API
     */
    public static final String INTERNAL_API_VERSION_3_TOKEN = "v3Internal";
    public static final String INTERNAL_API_VERSION_3 = "/" + INTERNAL_API_VERSION_3_TOKEN;
  }

  /**
   * Router Configuration.
   */
  public static final class Router {
    public static final String ADDRESS = "router.bind.address";
    public static final String ROUTER_PORT = "router.bind.port";
    public static final String ROUTER_SSL_PORT = "router.ssl.bind.port";
    public static final String BACKLOG_CONNECTIONS = "router.connection.backlog";
    public static final String SERVER_BOSS_THREADS = "router.server.boss.threads";
    public static final String SERVER_WORKER_THREADS = "router.server.worker.threads";
    public static final String CONNECTION_TIMEOUT_SECS = "router.connection.idle.timeout.secs";
    public static final String ROUTER_AUDIT_PATH_CHECK_ENABLED = "router.audit.path.check.enabled";
    public static final String ROUTER_AUDIT_LOG_ENABLED = "router.audit.log.enabled";

    /**
     * Defaults.
     */
    public static final String DEFAULT_ROUTER_PORT = "11015";

    public static final String DONT_ROUTE_SERVICE = "dont-route-to-service";
    public static final String AUDIT_LOGGER_NAME = "http-access";
  }

  /**
   * Metrics constants.
   */
  public static final class Metrics {
    public static final String ADDRESS = "metrics.bind.address";
    public static final String PORT = "metrics.bind.port";
    public static final String CLUSTER_NAME = "metrics.cluster.name";
    public static final String CONFIG_AUTHENTICATION_REQUIRED = "metrics.authenticate";
    public static final String BACKLOG_CONNECTIONS = "metrics.connection.backlog";
    public static final String EXEC_THREADS = "metrics.exec.threads";
    public static final String BOSS_THREADS = "metrics.boss.threads";
    public static final String WORKER_THREADS = "metrics.worker.threads";
    public static final String NUM_INSTANCES = "metrics.num.instances";
    public static final String NUM_CORES = "metrics.num.cores";
    public static final String MEMORY_MB = "metrics.memory.mb";
    public static final String MAX_INSTANCES = "metrics.max.instances";
    public static final String SERVICE_DESCRIPTION = "Service to handle metrics requests.";
    public static final String PROCESSOR_MAX_DELAY_MS = "metrics.processor.max.delay.ms";
    public static final String QUEUE_SIZE = "metrics.processor.queue.size";

    public static final String ENTITY_TABLE_NAME = "metrics.data.entity.tableName";
    public static final String METRICS_TABLE_PREFIX = "metrics.data.table.prefix";
    public static final String TIME_SERIES_TABLE_ROLL_TIME = "metrics.data.table.ts.rollTime";

    public static final String METRICS_MINIMUM_RESOLUTION_SECONDS = "metrics.minimum.resolution.seconds";
    public static final String MINIMUM_RESOLUTION_RETENTION_SECONDS =
      "metrics.data.table.retention.minimum.resolution.seconds";
    // Key prefix for retention seconds. The actual key is suffixed by the table resolution.
    public static final String RETENTION_SECONDS = "metrics.data.table.retention.resolution.";
    public static final int MINUTE_RESOLUTION = 60;
    public static final int HOUR_RESOLUTION = 3600;
    public static final long PROCESS_INTERVAL_MILLIS = 60000;
    public static final String RETENTION_SECONDS_SUFFIX = ".seconds";

    public static final String TOPIC_PREFIX = "metrics.topic.prefix";

    public static final String ADMIN_TOPIC = "metrics.admin.topic";
    public static final String ADMIN_POLL_DELAY_MILLIS = "metrics.admin.poll.delay.millis";

    // part of table name for metrics
    public static final String METRICS_META_TABLE = "metrics.meta.table";
    public static final String DEFAULT_ENTITY_TABLE_NAME = "metrics.entity";
    public static final String DEFAULT_METRIC_TABLE_PREFIX = "metrics.table";

    public static final String METRICS_HBASE_MAX_SCAN_THREADS = "metrics.hbase.max.scan.threads";
    public static final String METRICS_HBASE_TABLE_SPLITS = "metrics.table.splits";
    public static final String METRICS_TABLE_HBASE_SPLIT_POLICY = "metrics.table.hbase.split.policy";

    public static final int DEFAULT_TIME_SERIES_TABLE_ROLL_TIME = 3600;

    public static final String MESSAGING_TOPIC_NUM = "metrics.messaging.topic.num";

    public static final String TWILL_INSTANCE_ID = "metrics.twill.instance.id";

    public static final String METRICS_WRITER_EXTENSIONS_DIR = "metrics.writer.extensions.dir";
    public static final String METRICS_WRITER_PREFIX = "metrics.writer.";

    public static final Map<String, String> METRICS_PROCESSOR_CONTEXT =
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
                      Constants.Metrics.Tag.COMPONENT, Constants.Service.METRICS_PROCESSOR);

    public static final Map<String, String> TRANSACTION_MANAGER_CONTEXT =
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
                      Constants.Metrics.Tag.COMPONENT, Constants.Service.TRANSACTION);
    // metrics context for system storage
    public static final Map<String, String> STORAGE_METRICS_TAGS = ImmutableMap.of(
      Tag.COMPONENT, "system.storage",
      Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace());

    public static final String PROGRAM_METRICS_ENABLED = "app.program.metrics.enabled";
    public static final String STRUCTURED_TABLE_TIME_METRICS_ENABLED = "structured.table.time.metrics.enabled";

    /**
     * Metric's dataset related constants.
     */
    public static final class Dataset {
      /** Defines reporting interval for HBase stats, in seconds */
      public static final String HBASE_STATS_REPORT_INTERVAL = "metrics.dataset.hbase.stats.report.interval";
      /** Defines reporting interval for LevelDB stats, in seconds */
      public static final String LEVELDB_STATS_REPORT_INTERVAL = "metrics.dataset.leveldb.stats.report.interval";
    }

    /**
     * Metrics context tags
     */
    public static final class Tag {
      // NOTES:
      //   * tag names must be unique (keeping all possible here helps to ensure that)
      //   * tag names better be short to reduce the serialized metric value size

      public static final String NAMESPACE = "ns";

      public static final String RUN_ID = "run";
      public static final String INSTANCE_ID = "ins";

      public static final String COMPONENT = "cmp";
      public static final String HANDLER = "hnd";
      public static final String METHOD = "mtd";
      public static final String THREAD = "thd";

      public static final String DATASET = "ds";

      public static final String APP = "app";

      public static final String SERVICE = "srv";

      public static final String WORKER = "wrk";

      public static final String MAPREDUCE = "mr";
      public static final String MR_TASK_TYPE = "mrt";

      public static final String WORKFLOW = "wf";
      public static final String WORKFLOW_RUN_ID = "wfr";
      public static final String NODE = "nd";

      public static final String PROVISIONER = "prv";
      public static final String SPARK = "sp";
      public static final String STATUS = "st";

      // who emitted: user vs system (scope is historical name)
      public static final String SCOPE = "scp";

      public static final String PRODUCER = "pr";
      public static final String CONSUMER = "co";

      // For TMS
      public static final String TABLE = "tbl";
      public static final String TOPIC = "tpc";

      // For profile
      public static final String PROFILE = "pro";
      public static final String PROFILE_SCOPE = "psc";

      // For program
      public static final String PROGRAM = "prg";
      public static final String PROGRAM_TYPE = "prt";
      public static final String PROGRAM_ENTITY = "ent";
    }

    /**
     * Metric names
     */
    public static final class Name {

      /**
       * Service metrics
       */
      public static final class Service {
        public static final String SERVICE_INPUT = "system.requests.count";
        public static final String SERVICE_PROCESSED = "system.response.successful.count";
        public static final String SERVICE_EXCEPTIONS = "system.response.server.error.count";
      }

      /**
       * Dataset metrics
       */
      public static final class Dataset {
        public static final String READ_COUNT = "dataset.store.reads";
        public static final String OP_COUNT = "dataset.store.ops";
        public static final String WRITE_COUNT = "dataset.store.writes";
        public static final String WRITE_BYTES = "dataset.store.bytes";
      }

      /**
       * Logs metrics
       */
      public static final class Log {
        public static final String PROCESS_MIN_DELAY = "log.process.min.delay";
        public static final String PROCESS_MAX_DELAY = "log.process.max.delay";
        public static final String PROCESS_MESSAGES_COUNT = "log.process.message.count";
      }
    }

    /**
     * Metrics query constants and defaults
     */
    public static final class Query {
      public static final long MAX_HOUR_RESOLUTION_QUERY_INTERVAL = 36000;
      public static final long MAX_MINUTE_RESOLUTION_QUERY_INTERVAL = 600;
    }

    /**
     * Program metrics
     */
    public static final class Program {
      public static final String PROGRAM_COMPLETED_RUNS = "program.completed.runs";
      public static final String PROGRAM_FAILED_RUNS = "program.failed.runs";
      public static final String PROGRAM_KILLED_RUNS = "program.killed.runs";
      public static final String PROGRAM_REJECTED_RUNS = "program.rejected.runs";
      public static final String PROGRAM_NODE_MINUTES = "program.node.minutes";
      public static final String PROGRAM_PROVISIONING_DELAY_SECONDS = "program.provisioning.delay.seconds";
      public static final String RUN_TIME_SECONDS = "program.run.seconds";
      public static final String APPLICATION_COUNT = "application.count";
    }

    /**
     * Preview metrics
     */
    public static final class Preview {
      public static final String RUN_TIME_SECONDS = "preview.run.seconds";
    }

    /**
     * Structured table metrics
     */
    public static final class StructuredTable {

      public static final String METRICS_PREFIX = "structured.table.";
      public static final String TRANSACTION_COUNT = "structured.table.transaction.count";
      public static final String TRANSACTION_CONFLICT = "structured.table.transaction.conflict";
      public static final String ACTIVE_CONNECTIONS = "structured.table.connection.active";
      public static final String IDLE_CONNECTIONS = "structured.table.connection.idle";
      public static final String ERROR_CONNECTIONS = "structured.table.connection.error";
    }

    /**
     * Metadata storage metrics
     */
    public static final class MetadataStorage {
      public static final String METRICS_PREFIX = "metadata.storage.";
    }
  }

  /**
   * Configurations for metrics processor.
   */
  public static final class MetricsProcessor {
    public static final String NUM_INSTANCES = "metrics.processor.num.instances";
    public static final String NUM_CORES = "metrics.processor.num.cores";
    public static final String MEMORY_MB = "metrics.processor.memory.mb";
    public static final String MAX_INSTANCES = "metrics.processor.max.instances";

    public static final String METRICS_PROCESSOR_STATUS_HANDLER = "metrics.processor.status.handler";
    public static final String BIND_ADDRESS = "metrics.processor.status.bind.address";
    public static final String BIND_PORT = "metrics.processor.status.bind.port";

    public static final String SERVICE_DESCRIPTION = "Service to process application and system metrics.";
  }

  /**
   * Configurations for log query service.
   */
  public static final class LogQuery {
    public static final String ADDRESS = "log.query.server.bind.address";
    public static final String PORT = "log.query.server.bind.port";
  }

  /**
   * Configurations for log saver.
   */
  public static final class LogSaver {
    public static final String NUM_INSTANCES = "log.saver.num.instances";
    public static final String MEMORY_MB = "log.saver.container.memory.mb";
    public static final String NUM_CORES = "log.saver.container.num.cores";
    public static final String MAX_INSTANCES = "log.saver.max.instances";

    public static final String LOG_SAVER_STATUS_HANDLER = "log.saver.status.handler";
    public static final String ADDRESS = "log.saver.status.bind.address";

    public static final String SERVICE_DESCRIPTION = "Service to collect and store logs.";

    public static final String LOG_SAVER_INSTANCE_ID = "logSaverInstanceId";
    public static final String LOG_SAVER_INSTANCE_COUNT = "logSaverInstanceCount";
  }

  /**
   * Configurations for log buffer.
   */
  public static final class LogBuffer {
    // log buffer writer configs
    public static final String LOG_BUFFER_BASE_DIR = "log.buffer.base.dir";
    public static final String LOG_BUFFER_MAX_FILE_SIZE_BYTES = "log.buffer.max.file.size.bytes";
    // log buffer recovery configs
    public static final String LOG_BUFFER_RECOVERY_BATCH_SIZE = "log.buffer.recovery.batch.size";
    // number of events to be sent to time event queue processor from incoming queue
    public static final String LOG_BUFFER_PIPELINE_BATCH_SIZE = "log.buffer.pipeline.batch.size";
    // log buffer server configs
    public static final String LOG_BUFFER_SERVER_BIND_ADDRESS = "log.buffer.server.bind.address";
    public static final String LOG_BUFFER_SERVER_BIND_PORT = "log.buffer.server.bind.port";
  }

  /**
   * Monitor constants.
   */
  public static final class Monitor {
    public static final String STATUS_OK = "OK";
    public static final String STATUS_NOTOK = "NOTOK";
    public static final String DISCOVERY_TIMEOUT_SECONDS = "monitor.handler.service.discovery.timeout.seconds";
  }

  /**
   * Runtime Monitor constants.
   */
  public static final class RuntimeMonitor {
    public static final String SERVICE_DESCRIPTION = "Service for the program runtime system.";
    public static final String POLL_TIME_MS = "app.program.runtime.monitor.polltime.ms";
    public static final String BATCH_SIZE = "app.program.runtime.monitor.batch.size";
    public static final String TOPICS_CONFIGS = "app.program.runtime.monitor.topics.configs";
    public static final String GRACEFUL_SHUTDOWN_MS = "app.program.runtime.monitor.graceful.shutdown.ms";
    public static final String THREADS = "app.program.runtime.monitor.threads";
    public static final String INIT_BATCH_SIZE = "app.program.runtime.monitor.initialize.batch.size";
    public static final String RUN_RECORD_FETCHER_CLASS = "app.program.runtime.monitor.run.record.fetch.class";

    public static final String BIND_ADDRESS = "app.program.runtime.monitor.server.bind.address";
    public static final String BIND_PORT = "app.program.runtime.monitor.server.bind.port";
    public static final String SSL_ENABLED = "app.program.runtime.monitor.server.ssl.enabled";

    public static final String COMPRESSION_ENABLED = "app.program.runtime.monitor.compression.enabled";

    // Configuration key for specifying the base URL for sending monitoring messages.
    // If it is missing from the configuration, SSH tunnel will be used.
    public static final String MONITOR_URL = "app.program.runtime.monitor.url";
    public static final String MONITOR_TYPE_PREFIX = "app.program.runtime.monitor.type.";
    public static final String MONITOR_URL_AUTHENTICATOR_CLASS_PREFIX =
      "app.program.runtime.monitor.url.authenticator.class.";

    // Prefix for that configuration key for storing discovery endpoint in the format of "host:port"
    public static final String DISCOVERY_SERVICE_PREFIX = "app.program.runtime.discovery.service.";

    // Constants for secure connections
    public static final String SSH_USER = "ssh.user";
    public static final String PUBLIC_KEY = "id_rsa.pub";
    public static final String PRIVATE_KEY = "id_rsa";

    // Configurations related to the Service Proxy that runs in the remote runtime for proxying traffic from
    // remote cluster back into calling CDAP services.
    // File name that stores the service proxy information
    public static final String SERVICE_PROXY_FILE = "cdap.service.proxy.json";
    // Configuration key for the service proxy in the format of "host:port"
    public static final String SERVICE_PROXY_ADDRESS = "app.program.runtime.service.proxy.address";
    // Configuration key for the service proxy password. It is only used within a runtime cluster.
    public static final String SERVICE_PROXY_PASSWORD = "app.program.runtime.service.proxy.password";
    public static final String SERVICE_PROXY_PASSWORD_FILE = "cdap.service.proxy.secret";
  }

  /**
   * Logging constants.
   */
  public static final class Logging {
    public static final String COMPONENT_NAME = "services";

    // Configuration keys
    public static final String KAFKA_TOPIC = "log.kafka.topic";
    public static final String TMS_TOPIC_PREFIX = "log.tms.topic.prefix";
    public static final String APPENDER_QUEUE_SIZE = "log.queue.size";
    public static final String NUM_PARTITIONS = "log.publish.num.partitions";
    public static final String LOG_PUBLISH_PARTITION_KEY = "log.publish.partition.key";

    public static final String PIPELINE_CONFIG_DIR = "log.process.pipeline.config.dir";
    public static final String PIPELINE_LIBRARY_DIR = "log.process.pipeline.lib.dir";
    public static final String PIPELINE_AUTO_BUFFER_RATIO = "log.process.pipeline.auto.buffer.ratio";

    // The following properties can be defined in cdap-site and overridden in individual pipeline config xml
    public static final String PIPELINE_BUFFER_SIZE = "log.process.pipeline.buffer.size";
    public static final String PIPELINE_EVENT_DELAY_MS = "log.process.pipeline.event.delay.ms";
    public static final String PIPELINE_KAFKA_FETCH_SIZE = "log.process.pipeline.kafka.fetch.size";
    public static final String PIPELINE_CHECKPOINT_INTERVAL_MS = "log.process.pipeline.checkpoint.interval.ms";
    public static final String PIPELINE_LOGGER_CACHE_SIZE = "log.process.pipeline.logger.cache.size";
    public static final String PIPELINE_LOGGER_CACHE_EXPIRATION_MS = "log.process.pipeline.logger.cache.expiration.ms";

    // log appender configs
    public static final String LOG_APPENDER_PROVIDER = "app.program.log.appender.provider";
    public static final String LOG_APPENDER_PROVISIONERS = "app.program.log.appender.provisioners";
    public static final String LOG_APPENDER_EXT_DIR = "app.program.log.appender.extensions.dir";
    public static final String LOG_APPENDER_PROPERTY_PREFIX = "app.program.log.appender.system.properties.";

    // Property key in the logger context to indicate it is performing pipeline validation
    public static final String PIPELINE_VALIDATION = "log.pipeline.validation";

    public static final String SYSTEM_PIPELINE_CHECKPOINT_PREFIX = "cdap";

    // key constants
    public static final String TAG_NAMESPACE_ID = ".namespaceId";
    public static final String TAG_APPLICATION_ID = ".applicationId";
    public static final String TAG_RUN_ID = ".runId";
    public static final String TAG_INSTANCE_ID = ".instanceId";

    public static final String TAG_SERVICE_ID = ".serviceId";
    public static final String TAG_MAP_REDUCE_JOB_ID = ".mapReduceId";
    public static final String TAG_SPARK_JOB_ID = ".sparkId";
    public static final String TAG_USER_SERVICE_ID = ".userserviceid";
    public static final String TAG_HANDLER_ID = ".userhandlerid";
    public static final String TAG_WORKER_ID = ".workerid";
    public static final String TAG_WORKFLOW_ID = ".workflowId";
    public static final String TAG_WORKFLOW_MAP_REDUCE_ID = ".workflowMapReduceId";
    public static final String TAG_WORKFLOW_SPARK_ID = ".workflowSparkId";
    public static final String TAG_WORKFLOW_PROGRAM_RUN_ID = ".workflowProgramRunId";

    public static final String EVENT_TYPE_TAG = "MDC:eventType";
    public static final String USER_LOG_TAG_VALUE = "userLog";
  }

  /**
   * Security configuration.
   */
  public static final class Security {


    /** Enables security. */
    public static final String ENABLED = "security.enabled";
    /** Enables Kerberos authentication. */
    public static final String KERBEROS_ENABLED = "kerberos.auth.enabled";
    /** Kerberos keytab relogin interval. */
    public static final String KERBEROS_KEYTAB_RELOGIN_INTERVAL = "kerberos.auth.relogin.interval.seconds";
    /** Algorithm used to generate the digest for access tokens. */
    public static final String TOKEN_DIGEST_ALGO = "security.token.digest.algorithm";
    /** Key length for secret key used by token digest algorithm. */
    public static final String TOKEN_DIGEST_KEY_LENGTH = "security.token.digest.keylength";
    /** Time duration in milliseconds after which an active secret key should be retired. */
    public static final String TOKEN_DIGEST_KEY_EXPIRATION = "security.token.digest.key.expiration.ms";
    /** Parent znode used for secret key distribution in ZooKeeper. */
    public static final String DIST_KEY_PARENT_ZNODE = "security.token.distributed.parent.znode";
    /**
     * Comma separated URL's that clients should use to communicate with the Authentication Server.
     * Each URL should follow the format protocol://host:port. Leave empty to use the default URL generated by
     * the Authentication Server.
     */
    public static final String AUTH_SERVER_ANNOUNCE_URLS = "security.auth.server.announce.urls";

    /** Address the Authentication Server should bind to. */
    public static final String AUTH_SERVER_BIND_ADDRESS = "security.auth.server.bind.address";
    /** Configuration for External Authentication Server. */
    public static final String AUTH_SERVER_BIND_PORT = "security.auth.server.bind.port";
    /** Maximum number of handler threads for the Authentication Server embedded Jetty instance. */
    public static final String MAX_THREADS = "security.server.maxthreads";
    /** Access token expiration time in milliseconds. */
    public static final String TOKEN_EXPIRATION = "security.server.token.expiration.ms";
    /** Long lasting Access token expiration time in milliseconds. */
    public static final String EXTENDED_TOKEN_EXPIRATION = "security.server.extended.token.expiration.ms";
    public static final String CFG_FILE_BASED_KEYFILE_PATH = "security.data.keyfile.path";
    /** Configuration for security realm. */
    public static final String CFG_REALM = "security.realm";
    /** Authentication Handler class name */
    public static final String AUTH_HANDLER_CLASS = "security.authentication.handlerClassName";
    /** Prefix for all configurable properties of an Authentication handler. */
    public static final String AUTH_HANDLER_CONFIG_BASE = "security.authentication.handler.";
    /** Authentication Login Module class name */
    public static final String LOGIN_MODULE_CLASS_NAME = "security.authentication.loginmodule.className";
    /** Realm file for Basic Authentication */
    public static final String BASIC_REALM_FILE = "security.authentication.basic.realmfile";
    /** Configuration for specifying keytab location. The location will contain ${name} which will be replaced
     * by the user/owner of the entities name. */
    public static final String KEYTAB_PATH = "security.keytab.path";

    /** Key to specify the kerberos principal of the entity owner **/
    public static final String PRINCIPAL = "principal";

    /**
     * App Fabric
     */
    public static final class SSL {
      /** Enables SSL for external services. */
      @SuppressWarnings("unused")
      public static final String EXTERNAL_ENABLED = "ssl.external.enabled";
      /** Enables SSL for internal services. */
      public static final String INTERNAL_ENABLED = "ssl.internal.enabled";
      /** File path to certificate file in PEM format. */
      public static final String INTERNAL_CERT_PATH = "ssl.internal.cert.path";
      /** Password for the SSL certificate. */
      public static final String INTERNAL_CERT_PASSWORD = "ssl.internal.cert.password";
    }

    /**
     * Authorization.
     */
    public static final class Authorization {
      /** Enables authorization */
      public static final String ENABLED = "security.authorization.enabled";
      /** Extension jar path */
      public static final String EXTENSION_JAR_PATH = "security.authorization.extension.jar.path";
      /** Extra classpath for security extension **/
      public static final String EXTENSION_EXTRA_CLASSPATH = "security.authorization.extension.extra.classpath";
      /** Prefix for extension properties */
      public static final String EXTENSION_CONFIG_PREFIX =
        "security.authorization.extension.config.";
      /** TTL for entries in container's privilege cache */
      public static final String CACHE_TTL_SECS = "security.authorization.cache.ttl.secs";
      /** Maximum number of entries the authorization cache will hold */
      public static final String CACHE_MAX_ENTRIES = "security.authorization.cache.max.entries";
      /** Batch size for query for the visibility of entities */
      public static final int VISIBLE_BATCH_SIZE = 500;
      /** Upper limit on extension operation time after which the time is logged as WARN rather than TRACE */
      public static final String EXTENSION_OPERATION_TIME_WARN_THRESHOLD =
        "security.authorization.extension.operation.time.warn.threshold.ms";
    }

    /**
     * Secure Store
     */
    public static final class Store {
      /** Location of the secure store file. */
      public static final String FILE_PATH = "security.store.file.path";
      /** Name of the secure store file. */
      public static final String FILE_NAME = "security.store.file.name";
      /** Password to access the secure store. */
      public static final String FILE_PASSWORD = "security.store.file.password";
      /** Backend provider for the secure store. e.g. file */
      public static final String PROVIDER = "security.store.provider";
      /** Secure store extension dir*/
      public static final String EXTENSIONS_DIR = "security.store.extensions.dir";
      /**Secure store extension property prefix*/
      public static final String PROPERTY_PREFIX = "security.store.system.properties.";
    }

    /**
     * Headers for security.
     */
    public static final class Headers {
      /** Internal user ID header passed from Router to downstream services */
      public static final String USER_ID = "CDAP-UserId";
      /** User IP header passed from Router to downstream services */
      public static final String USER_IP = "CDAP-UserIP";
      /** User principal passed from program container to cdap service containers */
      public static final String USER_PRINCIPAL = "CDAP-User-Principal";
      /** program id passed from program container to cdap service containers */
      public static final String PROGRAM_ID = "CDAP-Program-Id";
    }

    /**
     * Security configuration for Router.
     */
    public static final class Router {
      /** SSL keystore location */
      public static final String SSL_KEYSTORE_PATH = "router.ssl.keystore.path";
      /** SSL keystore key password */
      public static final String SSL_KEYPASSWORD = "router.ssl.keystore.keypassword";
      /** SSL keystore password */
      public static final String SSL_KEYSTORE_PASSWORD = "router.ssl.keystore.password";
      /** Paths to exclude from authentication, given by a single regular expression */
      public static final String BYPASS_AUTHENTICATION_REGEX = "router.bypass.auth.regex";

      /** File path to certificate file in PEM format. */
      public static final String SSL_CERT_PATH = "router.ssl.cert.path";
      /** Password for the SSL certificate. */
      public static final String SSL_CERT_PASSWORD = "router.ssl.cert.password";
    }

    /**
     * Security configuration for ExternalAuthenticationServer.
     */
    public static final class AuthenticationServer {
      /** SSL port */
      public static final String SSL_PORT = "security.auth.server.ssl.bind.port";
      /** SSL keystore location */
      public static final String SSL_KEYSTORE_PATH = "security.auth.server.ssl.keystore.path";
      /** SSL keystore type */
      public static final String SSL_KEYSTORE_TYPE = "security.auth.server.ssl.keystore.type";
      /** SSL keystore key password */
      public static final String SSL_KEYPASSWORD = "security.auth.server.ssl.keystore.keypassword";
      /** SSL keystore password */
      public static final String SSL_KEYSTORE_PASSWORD = "security.auth.server.ssl.keystore.password";

      /** Default SSL keystore type */
      public static final String DEFAULT_SSL_KEYSTORE_TYPE = "JKS";

      /** SSL truststore location */
      public static final String SSL_TRUSTSTORE_PATH = "security.auth.server.ssl.truststore.path";
      /** SSL truststore type */
      public static final String SSL_TRUSTSTORE_TYPE = "security.auth.server.ssl.truststore.type";
      /** SSL truststore password */
      public static final String SSL_TRUSTSTORE_PASSWORD = "security.auth.server.ssl.truststore.password";
    }

    /** Path to the Kerberos keytab file used by CDAP master */
    public static final String CFG_CDAP_MASTER_KRB_KEYTAB_PATH = "cdap.master.kerberos.keytab";
    /** Kerberos principal used by CDAP master */
    public static final String CFG_CDAP_MASTER_KRB_PRINCIPAL = "cdap.master.kerberos.principal";

    public static final String UGI_CACHE_EXPIRATION_MS = "cdap.ugi.cache.expiration.ms";
  }

  /**
   * Explore module configuration.
   */
  public static final class Explore {
    public static final String CCONF_KEY = "explore.cconfiguration";
    public static final String HCONF_KEY = "explore.hconfiguration";
    public static final String TX_QUERY_KEY = "explore.hive.query.tx.id";
    public static final String TX_QUERY_CLOSED = "explore.hive.query.tx.commited";
    public static final String QUERY_ID = "explore.query.id";
    public static final String CONTAINER_YARN_APP_CLASSPATH_FIRST = "explore.container.yarn.app.classpath.first";

    public static final String START_ON_DEMAND = "explore.start.on.demand";
    public static final String DATASET_NAME = "explore.dataset.name";
    public static final String DATASET_NAMESPACE = "explore.dataset.namespace";
    public static final String PREVIEWS_DIR_NAME = "explore.previews.dir";
    public static final String CREDENTIALS_DIR_NAME = "explore.credentials.dir";

    // Older hive versions don't have the following defined so we cannot use conf.getVar or conf.setVar and
    // we need to hardcode it here so that we can use conf.get and conf.set instead.
    public static final String HIVE_SERVER2_SPNEGO_KEYTAB = "hive.server2.authentication.spnego.keytab";
    public static final String HIVE_SERVER2_SPNEGO_PRINCIPAL = "hive.server2.authentication.spnego.principal";
    public static final String SUBMITLOCALTASKVIACHILD = "hive.exec.submit.local.task.via.child";
    public static final String SUBMITVIACHILD = "hive.exec.submitviachild";
    public static final String HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST_APPEND =
      "hive.security.authorization.sqlstd.confwhitelist.append";
    // Same as YarnConfiguration.TIMELINE_SERVICE_ENABLED, which isn't available on all hadoop versions
    public static final String TIMELINE_SERVICE_ENABLED = "yarn.timeline-service.enabled";
    // Same as YarnConfiguration.TIMELINE_DELEGATION_KEY_UPDATE_INTERVAL, which isn't available on all hadoop versions
    public static final String TIMELINE_DELEGATION_KEY_UPDATE_INTERVAL =
      "yarn.timeline-service.delegation.key.update-interval";

    /** Determines how to behave when the Hive version is unsupported */
    public static final String HIVE_VERSION_RESOLUTION_STRATEGY = "hive.version.resolution.strategy";
    public static final String HIVE_AUTO_STRICT_VERSION = "auto.strict";
    public static final String HIVE_AUTO_LATEST_VERSION = "auto.latest";

    // a marker so that we know which tables are created by CDAP
    public static final String CDAP_NAME = "cdap.name";
    public static final String CDAP_VERSION = "cdap.version";

    public static final String SERVER_ADDRESS = "explore.service.bind.address";
    public static final String SERVER_PORT = "explore.service.bind.port";

    public static final String BACKLOG_CONNECTIONS = "explore.service.connection.backlog";
    public static final String EXEC_THREADS = "explore.service.exec.threads";
    public static final String WORKER_THREADS = "explore.service.worker.threads";

    /** Twill Runnable configuration **/
    public static final String CONTAINER_VIRTUAL_CORES = "explore.executor.container.num.cores";
    public static final String CONTAINER_MEMORY_MB = "explore.executor.container.memory.mb";

    public static final String LOCAL_DATA_DIR = "explore.local.data.dir";
    public static final String EXPLORE_ENABLED = "explore.enabled";
    public static final String WRITES_ENABLED = "explore.writes.enabled";

    public static final String ACTIVE_OPERATION_TIMEOUT_SECS = "explore.active.operation.timeout.secs";
    public static final String INACTIVE_OPERATION_TIMEOUT_SECS = "explore.inactive.operation.timeout.secs";
    public static final String CLEANUP_JOB_SCHEDULE_SECS = "explore.cleanup.job.schedule.secs";

    public static final String SERVICE_DESCRIPTION = "Service to run ad-hoc queries.";
    public static final String HTTP_TIMEOUT = "explore.http.timeout";

    public static final String HIVE_SERVER_JDBC_URL = "hive.server2.jdbc.url";
    public static final String HIVE_METASTORE_TOKEN_SIG = "hive.metastore.token.signature";
    public static final String HIVE_METASTORE_TOKEN_SERVICE_NAME = "hive.metastore.service";

    /**
     * Explore JDBC constants.
     */
    public static final class Jdbc {
      public static final String URL_PREFIX = "jdbc:cdap://";
    }
  }

  public static final String CFG_LOCAL_DATA_DIR = "local.data.dir";
  public static final String CFG_HDFS_USER = "hdfs.user";
  public static final String CFG_HDFS_NAMESPACE = "hdfs.namespace";
  public static final String CFG_HDFS_LIB_DIR = "hdfs.lib.dir";

  public static final String CFG_TWILL_ZK_NAMESPACE = "twill.zookeeper.namespace";
  public static final String CFG_TWILL_NO_CONTAINER_TIMEOUT = "twill.no.container.timeout";

  /**
   * Data Fabric.
   */
  public enum InMemoryPersistenceType {
    MEMORY,
    LEVELDB,
    HSQLDB
  }
  /** defines which persistence engine to use when running all in one JVM. **/
  public static final String CFG_DATA_INMEMORY_PERSISTENCE = "data.local.inmemory.persistence.type";
  public static final String CFG_DATA_LEVELDB_DIR = "data.local.storage";
  public static final String CFG_DATA_LEVELDB_BLOCKSIZE = "data.local.storage.blocksize";
  public static final String CFG_DATA_LEVELDB_CACHESIZE = "data.local.storage.cachesize";
  public static final String CFG_DATA_LEVELDB_FSYNC = "data.local.storage.fsync";

  /**
   * Defaults for Data Fabric.
   */
  public static final String DEFAULT_DATA_LEVELDB_DIR = "data";
  public static final int DEFAULT_DATA_LEVELDB_BLOCKSIZE = 1024;
  public static final long DEFAULT_DATA_LEVELDB_CACHESIZE = 1024 * 1024 * 100;
  public static final boolean DEFAULT_DATA_LEVELDB_FSYNC = true;

  /**
   * Used for upgrade and backwards compatability
   */
  public static final String DEVELOPER_ACCOUNT = "developer";

  /**
   * Constants related to external systems.
   */
  public static final class External {
    /**
     * Constants used by Java security.
     */
    public static final class JavaSecurity {
      public static final String ENV_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
    }

    /**
     * Constants used by ZooKeeper.
     */
    public static final class Zookeeper {
      public static final String ENV_AUTH_PROVIDER_1 = "zookeeper.authProvider.1";
      public static final String ENV_ALLOW_SASL_FAILED_CLIENTS = "zookeeper.allowSaslFailedClients";
    }
  }

  /**
   * Constants for the dashboard/frontend.
   */
  public static final class Dashboard {
    /**
     * Port for the dashboard to bind to in non-SSL mode.
     */
    public static final String BIND_PORT = "dashboard.bind.port";

    /**
     * Port for the dashboard to bind to in SSL mode.
     */
    public static final String SSL_BIND_PORT = "dashboard.ssl.bind.port";

  }

  /**
   * Constants for endpoints
   */
  public static final class EndPoints {
    /**
    * Status endpoint
    */
    public static final String STATUS = "/status";
  }

  /**
   * Constants for namespaces
   */
  public static final class Namespace {
    public static final String NAMESPACES_DIR = "namespaces.dir";
  }

  /**
   * Constants for metadata service and metadata migrator
   */
  public static final class Metadata {
    public static final String SERVICE_DESCRIPTION = "Service to perform metadata operations.";
    public static final String SERVICE_BIND_ADDRESS = "metadata.service.bind.address";
    public static final String SERVICE_BIND_PORT = "metadata.service.bind.port";
    public static final String SERVICE_WORKER_THREADS = "metadata.service.worker.threads";
    public static final String SERVICE_EXEC_THREADS = "metadata.service.exec.threads";
    public static final String HANDLERS_NAME = "metadata.handlers";
    public static final String MAX_CHARS_ALLOWED = "metadata.max.allowed.chars";

    public static final String MESSAGING_TOPIC = "metadata.messaging.topic";
    public static final String MESSAGING_FETCH_SIZE = "metadata.messaging.fetch.size";
    public static final String MESSAGING_POLL_DELAY_MILLIS = "metadata.messaging.poll.delay.millis";
    public static final String MESSAGING_RETRIES_ON_CONFLICT = "metadata.messaging.retries.on.conflict";

    public static final String STORAGE_PROVIDER_IMPLEMENTATION = "metadata.storage.implementation";
    public static final String STORAGE_PROVIDER_NOSQL = "nosql";
    public static final String STORAGE_PROVIDER_ELASTICSEARCH = "elastic";
  }

  /**
   * Constants for publishing audit
   */
  public static final class Audit {
    public static final String ENABLED = "audit.enabled";
    public static final String TOPIC = "audit.topic";
    public static final String PUBLISH_TIMEOUT_MS = "audit.publish.timeout.ms";
  }

  /**
   * Constants for the messaging system
   */
  public static final class MessagingSystem {
    public static final String SERVICE_DESCRIPTION = "Service for providing messaging system.";

    public static final String LOCAL_DATA_DIR = "messaging.local.data.dir";
    public static final String LOCAL_DATA_CLEANUP_FREQUENCY = "messaging.local.data.cleanup.frequency.secs";
    public static final String LOCAL_DATA_PARTITION_SECONDS = "messaging.local.data.partition.secs";

    public static final String CACHE_SIZE_MB = "messaging.cache.size.mb";

    public static final String HBASE_MAX_SCAN_THREADS = "messaging.hbase.max.scan.threads";
    public static final String HBASE_SCAN_CACHE_ROWS = "messaging.hbase.scan.cache.rows";
    public static final String METADATA_TABLE_NAME = "messaging.metadata.table.name";
    public static final String MESSAGE_TABLE_NAME = "messaging.message.table.name";
    public static final String MESSAGE_TABLE_HBASE_SPLITS = "messaging.message.table.hbase.splits";
    public static final String PAYLOAD_TABLE_NAME = "messaging.payload.table.name";
    public static final String PAYLOAD_TABLE_HBASE_SPLITS = "messaging.payload.table.hbase.splits";
    public static final String SYSTEM_TOPICS = "messaging.system.topics";
    public static final String TABLE_CACHE_EXPIRATION_SECONDS = "messaging.table.expiration.seconds";
    public static final String TABLE_HBASE_SPLIT_POLICY = "messaging.table.hbase.split.policy";
    public static final String TOPIC_DEFAULT_TTL_SECONDS = "messaging.topic.default.ttl.seconds";
    public static final String COPROCESSOR_METADATA_CACHE_UPDATE_FREQUENCY_SECONDS =
      "messaging.coprocessor.metadata.cache.update.frequency.seconds";

    public static final String HTTP_SERVER_WORKER_THREADS = "messaging.http.server.worker.threads";
    public static final String HTTP_SERVER_EXECUTOR_THREADS = "messaging.http.server.executor.threads";
    public static final String HTTP_SERVER_MAX_REQUEST_SIZE_MB = "messaging.http.server.max.request.size.mb";
    public static final String HTTP_SERVER_CONSUME_CHUNK_SIZE = "messaging.http.server.consume.chunk.size";
    public static final String HTTP_COMPRESS_PAYLOAD = "messaging.http.compress.payload";

    // Distributed mode related configurations
    public static final String HA_FENCING_DELAY_SECONDS = "messaging.ha.fencing.delay.seconds";
    public static final String CONTAINER_VIRTUAL_CORES = "messaging.container.num.cores";
    public static final String CONTAINER_MEMORY_MB = "messaging.container.memory.mb";
    public static final String CONTAINER_INSTANCES = "messaging.container.instances";
    public static final String MAX_INSTANCES = "messaging.max.instances";

    // The following configuration keys are set by messaging service TwillRunnable only,
    // not available in cdap-default.xml

    // Tell the instance id of the YARN container.
    public static final String CONTAINER_INSTANCE_ID = "messaging.container.instance.id";

    // The network address for the http server to bind to.
    public static final String HTTP_SERVER_BIND_ADDRESS = "messaging.http.server.bind.address";

    // The network port for the http server to bind to.
    public static final String HTTP_SERVER_BIND_PORT = "messaging.http.server.bind.port";

    // The guice binding name for http handler used by the messaging system
    public static final String HANDLER_BINDING_NAME = "messaging.http.handler";

    // The name of the HBase table attribute to store the bucket size being used by the RowKeyDistributor
    public static final String KEY_DISTRIBUTOR_BUCKETS_ATTR = "cdap.messaging.key.distributor.buckets";

    // TMS HBase table attribute that indicates the name of the TMS metadata table's HBase namespace
    public static final String HBASE_METADATA_TABLE_NAMESPACE = "cdap.messaging.metadata.hbase.namespace";

    // TMS HBase table attribute that indicates the number of prefix bytes used for the row key
    public static final String HBASE_MESSAGING_TABLE_PREFIX_NUM_BYTES = "cdap.messaging.table.prefix.num.bytes";
  }

  /**
   * Constants for operational stats
   */
  public static final class OperationalStats {
    public static final String EXTENSIONS_DIR = "operational.stats.extensions.dir";
    public static final String REFRESH_INTERVAL_SECS = "operational.stats.refresh.interval.secs";
  }

  /**
   * Constants for provisioners
   */
  public static final class Provisioner {
    public static final String EXTENSIONS_DIR = "runtime.extensions.dir";
    public static final String SYSTEM_PROPERTY_PREFIX = "provisioner.system.properties.";
    public static final String EXECUTOR_THREADS = "provisioner.executor.threads";
    public static final String CONTEXT_EXECUTOR_THREADS = "provisioner.context.executor.threads";
  }

  /**
   * Constants for Replication
   */
  public static final class Replication {
    public static final String CDAP_SHUTDOWN_TIME_FILENAME = "cdap_shutdown_time";
  }

  /**
   * Constants for retry policies.
   */
  public static final class Retry {
    private static final String PREFIX = "retry.policy.";
    public static final String TYPE = PREFIX + "type";
    public static final String MAX_TIME_SECS = PREFIX + "max.time.secs";
    public static final String MAX_RETRIES = PREFIX + "max.retries";
    public static final String DELAY_BASE_MS = PREFIX + "base.delay.ms";
    public static final String DELAY_MAX_MS = PREFIX + "max.delay.ms";
    public static final String MAPREDUCE_PREFIX = "mapreduce.";
    public static final String SPARK_PREFIX = "spark.";
    public static final String WORKFLOW_PREFIX = "workflow.";
    public static final String CUSTOM_ACTION_PREFIX = "custom.action.";
    public static final String WORKER_PREFIX = "worker.";
    public static final String SERVICE_PREFIX = "service.";
    public static final int LOCAL_DATASET_OPERATION_RETRY_DELAY_SECONDS = 5;
  }

  /**
   * Constants for HBase DDL executor
   */
  public static final class HBaseDDLExecutor {
    public static final String EXTENSIONS_DIR = "hbase.ddlexecutor.extension.dir";
  }

  /**
   * Constants for upgrade tool.
   */
  public static final class Upgrade {
    public static final String UPGRADE_THREAD_POOL_SIZE = "upgrade.thread.pool.size";
  }

  /**
   * Constants for field lineage
   */
  public static final class FieldLineage {
    /**
     * Direction for lineage
     */
    public enum Direction {
      INCOMING,
      OUTGOING,
      BOTH
    }
  }

  /**
   * Constants for profile
   */
  public static final class Profile {
    private static final String PREFIX = "profile.";
    /**
     * Whether or not to allow creating new profiles.
     */
    public static final String UPDATE_ALLOWED = PREFIX + "update.allowed";
  }

  /**
   * Constants for capability management
   */
  public static final class Capability {
    /**
     * Interval for scanning config
     */
    public static final String DIR_SCAN_INTERVAL_MINUTES = "capability.dir.scan.interval.minutes";
    /**
     * Capability config directory path key
     */
    public static final String CONFIG_DIR = "capability.config.dir";
  }
}
