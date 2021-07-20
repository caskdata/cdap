/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.test.app;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.dataset.DataSetException;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.service.AbstractService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpContentConsumer;
import io.cdap.cdap.api.service.http.HttpContentProducer;
import io.cdap.cdap.api.service.http.HttpServiceContext;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.JavaSparkMain;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.api.worker.WorkerContext;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import io.cdap.cdap.api.workflow.WorkflowContext;
import io.cdap.cdap.test.RevealingTxSystemClient;
import io.cdap.cdap.test.RevealingTxSystemClient.RevealingTransaction;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.entity.ContentType;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * An app that starts transactions with custom timeout and validates the timeout using a custom dataset.
 * This app also has methods with @TransactionPolicy annotations, to validate that these don't get run inside a tx.
 * These methods will then start transactions explicitly, and attempt to nest transactions.
 *
 * This relies on TestBase to inject {@link RevealingTxSystemClient} for this test.
 */
@SuppressWarnings("WeakerAccess")
public class AppWithCustomTx extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(AppWithCustomTx.class);

  private static final String NAME = "AppWithCustomTx";
  static final String DONE_DIR = "done.dir";
  static final String CAPTURE = "capture";
  static final String DEFAULT = "default";
  static final String FAILED = "failed";
  static final String FAIL_CONSUMER = "fail-consumer";
  static final String FAIL_PRODUCER = "fail-producer";

  static final String ACTION_TX = "TxAction";
  static final String ACTION_NOTX = "NoTxAction";
  static final String CONSUMER_TX = "TxContentConsumer";
  static final String CONSUMER_NOTX = "NoTxContentConsumer";
  static final String HANDLER_TX = "TxHandler";
  static final String HANDLER_NOTX = "NoTxHandler";
  static final String MAPREDUCE_NOTX = "NoTxMR";
  static final String MAPREDUCE_TX = "TxMR";
  static final String PRODUCER_TX = "TxContentProducer";
  static final String PRODUCER_NOTX = "NoTxContentProducer";
  static final String SERVICE = "TimedTxService";
  static final String SPARK_NOTX = "NoTxSpark";
  static final String SPARK_TX = "TxSpark";
  static final String WORKER_TX = "TxWorker";
  static final String WORKER_NOTX = "NoTxWorker";
  static final String WORKFLOW_TX = "TxWorkflow";
  static final String WORKFLOW_NOTX = "NoTxWorkflow";

  static final String INITIALIZE = "initialize";
  static final String INITIALIZE_TX = "initialize-tx";
  static final String INITIALIZE_TX_D = "initialize-tx-default";
  static final String INITIALIZE_NEST = "initialize-nest";
  static final String DESTROY = "destroy";
  static final String DESTROY_TX = "destroy-tx";
  static final String DESTROY_TX_D = "destroy-tx-default";
  static final String DESTROY_NEST = "destroy-nest";
  static final String ONERROR = "error";
  static final String ONERROR_TX = "error-tx";
  static final String ONERROR_TX_D = "error-tx-default";
  static final String ONERROR_NEST = "error-nest";
  static final String RUNTIME = "runtime";
  static final String RUNTIME_TX = "runtime-tx";
  static final String RUNTIME_TX_D = "runtime-tx-default";
  static final String RUNTIME_TX_T = "runtime-tx-tx";
  static final String RUNTIME_NEST = "runtime-nest";
  static final String RUNTIME_NEST_T = "runtime-nest-tx";
  static final String RUNTIME_NEST_CT = "runtime-nest-cxt-tx";
  static final String RUNTIME_NEST_TC = "runtime-nest-tx-cxt";

  static final int TIMEOUT_ACTION_RUNTIME = 13;
  static final int TIMEOUT_ACTION_DESTROY = 14;
  static final int TIMEOUT_ACTION_INITIALIZE = 15;
  static final int TIMEOUT_CONSUMER_DESTROY = 16;
  static final int TIMEOUT_CONSUMER_ERROR = 33;
  static final int TIMEOUT_CONSUMER_RUNTIME = 34;
  static final int TIMEOUT_HANDLER_DESTROY = 31;
  static final int TIMEOUT_HANDLER_INITIALIZE = 32;
  static final int TIMEOUT_HANDLER_RUNTIME = 29;
  static final int TIMEOUT_MAPREDUCE_DESTROY = 19;
  static final int TIMEOUT_MAPREDUCE_INITIALIZE = 20;
  static final int TIMEOUT_PRODUCER_DESTROY = 35;
  static final int TIMEOUT_PRODUCER_ERROR = 36;
  static final int TIMEOUT_PRODUCER_RUNTIME = 21;
  static final int TIMEOUT_SPARK_DESTROY = 22;
  static final int TIMEOUT_SPARK_INITIALIZE = 23;
  static final int TIMEOUT_WORKER_DESTROY = 24;
  static final int TIMEOUT_WORKER_INITIALIZE = 25;
  static final int TIMEOUT_WORKER_RUNTIME = 26;
  static final int TIMEOUT_WORKFLOW_DESTROY = 27;
  static final int TIMEOUT_WORKFLOW_INITIALIZE = 28;

  @Override
  public void configure() {
    setName(NAME);
    createDataset(CAPTURE, TransactionCapturingTable.class);
    addWorker(new NoTxWorker());
    addWorker(new TxWorker());
    addMapReduce(new TxMR());
    addMapReduce(new NoTxMR());
    addSpark(new TxSpark());
    addSpark(new NoTxSpark());
    addWorkflow(new TxWorkflow());
    addWorkflow(new NoTxWorkflow());
    addService(new AbstractService() {
      @Override
      protected void configure() {
        setName(SERVICE);
        addHandler(new TxHandler());
        addHandler(new NoTxHandler());
      }
    });
  }

  /**
   * Uses the provided Transactional with the given timeout, and records the timeout that the transaction
   * was actually given, or "default" if no explicit timeout was given.
   */
  static void executeRecordTransaction(Transactional transactional,
                                       final String row, final String column, int timeout) {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          recordTransaction(context, row, column + "-default"); // append _D for default timeout
        }
      });
    } catch (TransactionFailureException e) {
      throw Throwables.propagate(e);
    }
    try {
      transactional.execute(timeout, new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          recordTransaction(context, row, column);
        }
      });
    } catch (TransactionFailureException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * If in a transaction, records the timeout that the current transaction was given, or "default" if no explicit
   * timeout was given. Otherwise does nothing.
   *
   * Note: we know whether and what explicit timeout was given, because we inject a {@link RevealingTxSystemClient},
   *       which returns a {@link RevealingTransaction} for {@link TransactionSystemClient#startShort(int)} only.
   */
  static void recordTransaction(DatasetContext context, String row, String column) {
    TransactionCapturingTable capture = context.getDataset(CAPTURE);
    Transaction tx = capture.getTx();
    // we cannot cast because the RevealingTransaction is not visible in the program class loader
    String value = DEFAULT;
    if (tx == null) {
      try {
        capture.getTable().put(new Put(row, column, value));
        throw new RuntimeException("put to table without transaction should have failed.");
      } catch (DataSetException e) {
        // expected
      }
      return;
    }
    if ("RevealingTransaction".equals(tx.getClass().getSimpleName())) {
      int txTimeout;
      try {
        txTimeout = (int) tx.getClass().getField("timeout").get(tx);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
      value = String.valueOf(txTimeout);
    }
    capture.getTable().put(new Put(row, column, value));
  }

  /**
   * Attempt to nest transactions. we expect this to fail, and if it does, we write the value "failed"
   * to the table, for the test case to validate.
   */
  static void attemptNestedTransaction(Transactional txnl, final String row, final String key) {
    try {
      txnl.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          recordTransaction(ctext, row, key);
        }
      });
      LOG.error("Nested transaction should not have succeeded for {}:{}", row, key);
    } catch (TransactionFailureException e) {
      // expected: starting nested transaction should fail
      LOG.info("Nested transaction failed as expected for {}:{}", row, key);
    } catch (RuntimeException e) {
      // TODO (CDAP-6837): this is needed because worker's execute() propagates the tx failure as a runtime exception
      if (e.getCause() instanceof TransactionFailureException) {
        // expected: starting nested transaction should fail
        LOG.info("Nested transaction failed as expected for {}:{}", row, key);
      } else {
        throw e;
      }
    }
    // we know that the transactional is a program context and hence implement DatasetContext
    TransactionCapturingTable capture = ((DatasetContext) txnl).getDataset(CAPTURE);
    capture.getTable().put(new Put(row, key, FAILED));
  }

  /**
   * Execute a new transaction and attempt a nested transaction form there.
   * The nested transaction is executed with the same transactional.
   */
  static void executeAttemptNestedTransaction(final Transactional txnl, final String row, final String key) {
    executeAttemptNestedTransaction(txnl, txnl, row, key);
  }

  /**
   * Execute a new transaction and attempt a nested transaction form there.
   * The nested transaction can be executed through a different transactional.
   */
  static void executeAttemptNestedTransaction(Transactional txnl, final Transactional nestingTxnl,
                                              final String row, final String key) {
    try {
      txnl.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(nestingTxnl, row, key);
        }
      });
    } catch (TransactionFailureException e) {
      throw Throwables.propagate(e.getCause() == null ? e : e.getCause());
    }
  }

  public static class NoTxWorker extends AbstractWorker {

    @Override
    protected void configure() {
      setName(WORKER_NOTX);
    }

    @Override
    public void initialize(WorkerContext context) throws Exception {
      super.initialize(context);
      recordTransaction(getContext(), WORKER_NOTX, INITIALIZE);
      executeRecordTransaction(context, WORKER_NOTX, INITIALIZE_TX, TIMEOUT_WORKER_INITIALIZE);
      executeAttemptNestedTransaction(getContext(), WORKER_NOTX, INITIALIZE_NEST);
    }

    @Override
    public void run() {
      recordTransaction(getContext(), WORKER_NOTX, RUNTIME);
      executeRecordTransaction(getContext(), WORKER_NOTX, RUNTIME_TX, TIMEOUT_WORKER_RUNTIME);
      executeAttemptNestedTransaction(getContext(), WORKER_NOTX, RUNTIME_NEST);
    }

    @Override
    public void destroy() {
      recordTransaction(getContext(), WORKER_NOTX, DESTROY);
      executeRecordTransaction(getContext(), WORKER_NOTX, DESTROY_TX, TIMEOUT_WORKER_DESTROY);
      executeAttemptNestedTransaction(getContext(), WORKER_NOTX, DESTROY_NEST);
      super.destroy();
    }
  }

  public static class TxWorker extends AbstractWorker {

    @Override
    protected void configure() {
      setName(WORKER_TX);
    }

    @Override
    @TransactionPolicy(TransactionControl.IMPLICIT)
    public void initialize(WorkerContext context) throws Exception {
      super.initialize(context);
      recordTransaction(getContext(), WORKER_TX, INITIALIZE);
      attemptNestedTransaction(getContext(), WORKER_TX, INITIALIZE_NEST);
    }

    @Override
    public void run() {
      recordTransaction(getContext(), WORKER_TX, RUNTIME);
      executeRecordTransaction(getContext(), WORKER_TX, RUNTIME_TX, TIMEOUT_WORKER_RUNTIME);
      executeAttemptNestedTransaction(getContext(), WORKER_TX, RUNTIME_NEST);
    }

    @Override
    @TransactionPolicy(TransactionControl.IMPLICIT)
    public void destroy() {
      recordTransaction(getContext(), WORKER_TX, DESTROY);
      attemptNestedTransaction(getContext(), WORKER_TX, DESTROY_NEST);
      super.destroy();
    }
  }

  public static class TxHandler extends AbstractHttpServiceHandler {

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      super.initialize(context);
      recordTransaction(getContext(), HANDLER_TX, INITIALIZE);
      attemptNestedTransaction(getContext(), HANDLER_TX, INITIALIZE_NEST);
    }

    @Override
    public void destroy() {
      recordTransaction(getContext(), HANDLER_TX, DESTROY);
      attemptNestedTransaction(getContext(), HANDLER_TX, DESTROY_NEST);
      super.destroy();
    }

    @PUT
    @Path("tx")
    public HttpContentConsumer tx(HttpServiceRequest request, HttpServiceResponder responder) {
      recordTransaction(getContext(), HANDLER_TX, RUNTIME);
      attemptNestedTransaction(getContext(), HANDLER_TX, RUNTIME_NEST);

      return new HttpContentConsumer() {

        String body = null;

        @Override
        public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
          body = Bytes.toString(chunk);
          if (FAIL_CONSUMER.equals(body)) {
            LOG.warn("Failing consumer because the request was '" + body + "'");
            throw new RuntimeException(body);
          }
          recordTransaction(getContext(), CONSUMER_TX, RUNTIME);
          executeRecordTransaction(getContext(), CONSUMER_TX, RUNTIME_TX, TIMEOUT_CONSUMER_RUNTIME);
          executeRecordTransaction(transactional, CONSUMER_TX, RUNTIME_TX_T, TIMEOUT_CONSUMER_RUNTIME);
          executeAttemptNestedTransaction(getContext(), CONSUMER_TX, RUNTIME_NEST);
          executeAttemptNestedTransaction(transactional, CONSUMER_TX, RUNTIME_NEST_T);
          executeAttemptNestedTransaction(getContext(), transactional, CONSUMER_TX, RUNTIME_NEST_CT);
          executeAttemptNestedTransaction(transactional, getContext(), CONSUMER_TX, RUNTIME_NEST_TC);
        }

        @Override
        public void onError(HttpServiceResponder responder, Throwable failureCause) {
          recordTransaction(getContext(), CONSUMER_TX, ONERROR);
          attemptNestedTransaction(getContext(), CONSUMER_TX, ONERROR_NEST);
          responder.sendStatus(HttpURLConnection.HTTP_INTERNAL_ERROR);
        }

        @Override
        public void onFinish(HttpServiceResponder responder) throws Exception {

          recordTransaction(getContext(), CONSUMER_TX, DESTROY);
          attemptNestedTransaction(getContext(), CONSUMER_TX, DESTROY_NEST);

          responder.send(200, new HttpContentProducer() {

            @Override
            public ByteBuffer nextChunk(Transactional transactional) throws Exception {
              if (FAIL_PRODUCER.equals(body)) {
                LOG.warn("Failing producer because the request was '" + body + "'");
                throw new RuntimeException(body);
              }
              recordTransaction(getContext(), PRODUCER_TX, RUNTIME);
              executeRecordTransaction(getContext(), PRODUCER_TX, RUNTIME_TX, TIMEOUT_PRODUCER_RUNTIME);
              executeRecordTransaction(transactional, PRODUCER_TX, RUNTIME_TX_T, TIMEOUT_PRODUCER_RUNTIME);
              executeAttemptNestedTransaction(getContext(), PRODUCER_TX, RUNTIME_NEST);
              executeAttemptNestedTransaction(transactional, PRODUCER_TX, RUNTIME_NEST_T);
              executeAttemptNestedTransaction(getContext(), transactional, PRODUCER_TX, RUNTIME_NEST_CT);
              executeAttemptNestedTransaction(transactional, getContext(), PRODUCER_TX, RUNTIME_NEST_TC);
              return ByteBuffer.allocate(0);
            }

            @Override
            public void onFinish() throws Exception {
              recordTransaction(getContext(), PRODUCER_TX, DESTROY);
              attemptNestedTransaction(getContext(), PRODUCER_TX, DESTROY_NEST);
              File doneFile = new File(getContext().getRuntimeArguments().get(DONE_DIR), "tx" + body);
              doneFile.createNewFile();
            }

            @Override
            public void onError(Throwable failureCause) {
              recordTransaction(getContext(), PRODUCER_TX, ONERROR);
              attemptNestedTransaction(getContext(), PRODUCER_TX, ONERROR_NEST);
              File doneFile = new File(getContext().getRuntimeArguments().get(DONE_DIR), "tx" + body);
              try {
                doneFile.createNewFile();
              } catch (IOException e) {
                LOG.error("Failed to create file {}", doneFile.getAbsolutePath());
              }
            }
          }, ContentType.TEXT_PLAIN.getMimeType());
        }
      };
    }
  }

  public static class NoTxHandler extends AbstractHttpServiceHandler {

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void initialize(HttpServiceContext context) throws Exception {
      super.initialize(context);
      recordTransaction(getContext(), HANDLER_NOTX, INITIALIZE);
      executeRecordTransaction(getContext(), HANDLER_NOTX, INITIALIZE_TX, TIMEOUT_HANDLER_INITIALIZE);
      executeAttemptNestedTransaction(getContext(), HANDLER_NOTX, INITIALIZE_NEST);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void destroy() {
      recordTransaction(getContext(), HANDLER_NOTX, DESTROY);
      executeRecordTransaction(getContext(), HANDLER_NOTX, DESTROY_TX, TIMEOUT_HANDLER_DESTROY);
      executeAttemptNestedTransaction(getContext(), HANDLER_NOTX, DESTROY_NEST);
      super.destroy();
    }

    @PUT
    @Path("notx")
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public HttpContentConsumer notx(HttpServiceRequest request, HttpServiceResponder responder)
      throws TransactionFailureException {
      recordTransaction(getContext(), HANDLER_NOTX, RUNTIME);
      executeRecordTransaction(getContext(), HANDLER_NOTX, RUNTIME_TX, TIMEOUT_HANDLER_RUNTIME);
      executeAttemptNestedTransaction(getContext(), HANDLER_NOTX, RUNTIME_NEST);
      return new HttpContentConsumer() {

        String body = null;

        @Override
        public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
          body = Bytes.toString(chunk);
          if (FAIL_CONSUMER.equals(body)) {
            LOG.warn("Failing consumer because the request was '" + body + "'");
            throw new RuntimeException(body);
          }
          recordTransaction(getContext(), CONSUMER_NOTX, RUNTIME);
          executeRecordTransaction(getContext(), CONSUMER_NOTX, RUNTIME_TX, TIMEOUT_CONSUMER_RUNTIME);
          executeRecordTransaction(transactional, CONSUMER_NOTX, RUNTIME_TX_T, TIMEOUT_CONSUMER_RUNTIME);
          executeAttemptNestedTransaction(getContext(), CONSUMER_NOTX, RUNTIME_NEST);
          executeAttemptNestedTransaction(transactional, CONSUMER_NOTX, RUNTIME_NEST_T);
          executeAttemptNestedTransaction(getContext(), transactional, CONSUMER_NOTX, RUNTIME_NEST_CT);
          executeAttemptNestedTransaction(transactional, getContext(), CONSUMER_NOTX, RUNTIME_NEST_TC);
        }

        @Override
        @TransactionPolicy(TransactionControl.EXPLICIT)
        public void onError(HttpServiceResponder responder, Throwable failureCause) {
          recordTransaction(getContext(), CONSUMER_NOTX, ONERROR);
          executeRecordTransaction(getContext(), CONSUMER_NOTX, ONERROR_TX, TIMEOUT_CONSUMER_ERROR);
          executeAttemptNestedTransaction(getContext(), CONSUMER_NOTX, ONERROR_NEST);
          responder.sendStatus(HttpURLConnection.HTTP_INTERNAL_ERROR);
        }

        @Override
        @TransactionPolicy(TransactionControl.EXPLICIT)
        public void onFinish(HttpServiceResponder responder) throws Exception {

          recordTransaction(getContext(), CONSUMER_NOTX, DESTROY);
          executeRecordTransaction(getContext(), CONSUMER_NOTX, DESTROY_TX, TIMEOUT_CONSUMER_DESTROY);
          executeAttemptNestedTransaction(getContext(), CONSUMER_NOTX, DESTROY_NEST);
          responder.send(200, new HttpContentProducer() {

            @Override
            public ByteBuffer nextChunk(Transactional transactional) throws Exception {
              if (FAIL_PRODUCER.equals(body)) {
                LOG.warn("Failing producer because the request was '" + body + "'");
                throw new RuntimeException(body);
              }
              recordTransaction(getContext(), PRODUCER_NOTX, RUNTIME);
              executeRecordTransaction(getContext(), PRODUCER_NOTX, RUNTIME_TX, TIMEOUT_PRODUCER_RUNTIME);
              executeRecordTransaction(transactional, PRODUCER_NOTX, RUNTIME_TX_T, TIMEOUT_PRODUCER_RUNTIME);
              executeAttemptNestedTransaction(getContext(), PRODUCER_NOTX, RUNTIME_NEST);
              executeAttemptNestedTransaction(transactional, PRODUCER_NOTX, RUNTIME_NEST_T);
              executeAttemptNestedTransaction(getContext(), transactional, PRODUCER_NOTX, RUNTIME_NEST_CT);
              executeAttemptNestedTransaction(transactional, getContext(), PRODUCER_NOTX, RUNTIME_NEST_TC);
              return ByteBuffer.allocate(0);
            }

            @Override
            @TransactionPolicy(TransactionControl.EXPLICIT)
            public void onFinish() throws Exception {
              recordTransaction(getContext(), PRODUCER_NOTX, DESTROY);
              executeRecordTransaction(getContext(), PRODUCER_NOTX, DESTROY_TX, TIMEOUT_PRODUCER_DESTROY);
              executeAttemptNestedTransaction(getContext(), PRODUCER_NOTX, DESTROY_NEST);
              // onFinish and onError methods are called on the listener once the response is sent completely.
              // We want caller to wait till these callbacks finish.
              File doneFile = new File(getContext().getRuntimeArguments().get(DONE_DIR), "notx" + body);
              doneFile.createNewFile();
            }

            @Override
            @TransactionPolicy(TransactionControl.EXPLICIT)
            public void onError(Throwable failureCause) {
              recordTransaction(getContext(), PRODUCER_NOTX, ONERROR);
              executeRecordTransaction(getContext(), PRODUCER_NOTX, ONERROR_TX, TIMEOUT_PRODUCER_ERROR);
              executeAttemptNestedTransaction(getContext(), PRODUCER_NOTX, ONERROR_NEST);
              File doneFile = new File(getContext().getRuntimeArguments().get(DONE_DIR), "notx" + body);
              try {
                doneFile.createNewFile();
              } catch (IOException e) {
                LOG.error("Failed to create file {}", doneFile.getAbsolutePath());
              }
            }
          }, ContentType.TEXT_PLAIN.getMimeType());
        }
      };
    }
  }

  private static class TxWorkflow extends AbstractWorkflow {
    @Override
    protected void configure() {
      setName(WORKFLOW_TX);
      addAction(new TxAction());
    }

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      recordTransaction(context, WORKFLOW_TX, INITIALIZE);
      attemptNestedTransaction(context, WORKFLOW_TX, INITIALIZE_NEST);
    }

    @Override
    public void destroy() {
      super.destroy();
      recordTransaction(getContext(), WORKFLOW_TX, DESTROY);
      attemptNestedTransaction(getContext(), WORKFLOW_TX, DESTROY_NEST);
    }
  }

  private static class NoTxWorkflow extends AbstractWorkflow {
    @Override
    protected void configure() {
      setName(WORKFLOW_NOTX);
      addAction(new NoTxAction());
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      recordTransaction(context, WORKFLOW_NOTX, INITIALIZE);
      executeRecordTransaction(getContext(), WORKFLOW_NOTX, INITIALIZE_TX, TIMEOUT_WORKFLOW_INITIALIZE);
      executeAttemptNestedTransaction(getContext(), WORKFLOW_NOTX, INITIALIZE_NEST);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void destroy() {
      super.destroy();
      recordTransaction(getContext(), WORKFLOW_NOTX, DESTROY);
      executeRecordTransaction(getContext(), WORKFLOW_NOTX, DESTROY_TX, TIMEOUT_WORKFLOW_DESTROY);
      executeAttemptNestedTransaction(getContext(), WORKFLOW_NOTX, DESTROY_NEST);
    }
  }

  public static class NoTxAction extends AbstractCustomAction {

    @Override
    protected void configure() {
      setName(ACTION_NOTX);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    protected void initialize() throws Exception {
      recordTransaction(getContext(), ACTION_NOTX, INITIALIZE);
      executeRecordTransaction(getContext(), ACTION_NOTX, INITIALIZE_TX, TIMEOUT_ACTION_INITIALIZE);
      executeAttemptNestedTransaction(getContext(), ACTION_NOTX, INITIALIZE_NEST);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void destroy() {
      recordTransaction(getContext(), ACTION_NOTX, DESTROY);
      executeRecordTransaction(getContext(), ACTION_NOTX, DESTROY_TX, TIMEOUT_ACTION_DESTROY);
      executeAttemptNestedTransaction(getContext(), ACTION_NOTX, DESTROY_NEST);
    }

    @Override
    public void run() throws Exception {
      recordTransaction(getContext(), ACTION_NOTX, RUNTIME_TX);
      executeRecordTransaction(getContext(), ACTION_NOTX, RUNTIME_TX, TIMEOUT_ACTION_RUNTIME);
      executeAttemptNestedTransaction(getContext(), ACTION_NOTX, RUNTIME_NEST);
    }
  }

  public static class TxAction extends AbstractCustomAction {

    @Override
    protected void configure() {
      setName(ACTION_TX);
    }

    @Override
    protected void initialize() throws Exception {
      recordTransaction(getContext(), ACTION_TX, INITIALIZE);
      attemptNestedTransaction(getContext(), ACTION_TX, INITIALIZE_NEST);
    }

    @Override
    public void destroy() {
      recordTransaction(getContext(), ACTION_TX, DESTROY);
      attemptNestedTransaction(getContext(), ACTION_TX, DESTROY_NEST);
    }

    @Override
    public void run() throws Exception {
      recordTransaction(getContext(), ACTION_TX, RUNTIME_TX);
      executeRecordTransaction(getContext(), ACTION_TX, RUNTIME_TX, TIMEOUT_ACTION_RUNTIME);
      executeAttemptNestedTransaction(getContext(), ACTION_TX, RUNTIME_NEST);
    }
  }

  public static class NoTxMR extends AbstractMapReduce {
    @Override
    protected void configure() {
      setName(MAPREDUCE_NOTX);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    protected void initialize() throws Exception {
      // this job will fail because we don't configure the inputs etc. That is fine because destroy() still gets called
      recordTransaction(getContext(), MAPREDUCE_NOTX, INITIALIZE);
      executeRecordTransaction(getContext(), MAPREDUCE_NOTX, INITIALIZE_TX, TIMEOUT_MAPREDUCE_INITIALIZE);
      executeAttemptNestedTransaction(getContext(), MAPREDUCE_NOTX, INITIALIZE_NEST);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void destroy() {
      recordTransaction(getContext(), MAPREDUCE_NOTX, DESTROY);
      executeRecordTransaction(getContext(), MAPREDUCE_NOTX, DESTROY_TX, TIMEOUT_MAPREDUCE_DESTROY);
      executeAttemptNestedTransaction(getContext(), MAPREDUCE_NOTX, DESTROY_NEST);
    }
  }

  public static class TxMR extends AbstractMapReduce {
    @Override
    protected void configure() {
      setName(MAPREDUCE_TX);
    }

    @Override
    protected void initialize() throws Exception {
      // this job will fail because we don't configure the inputs etc. That is fine because destroy() still gets called
      recordTransaction(getContext(), MAPREDUCE_TX, INITIALIZE);
      attemptNestedTransaction(getContext(), MAPREDUCE_TX, INITIALIZE_NEST);
    }

    @Override
    public void destroy() {
      recordTransaction(getContext(), MAPREDUCE_TX, DESTROY);
      attemptNestedTransaction(getContext(), MAPREDUCE_TX, DESTROY_NEST);
    }
  }

  public static class NoOpMapper extends Mapper<Void, Void, Void, Void> {
    @Override
    protected void map(Void key, Void value, Context context) throws IOException, InterruptedException {
      // no-op
    }
  }

  public static class SingleRecordSplit extends InputSplit implements Writable {
    @Override
    public long getLength() throws IOException, InterruptedException {
      return 1;
    }
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }
    @Override
    public void write(DataOutput out) throws IOException {
    }
    @Override
    public void readFields(DataInput in) throws IOException {
    }
  }

  private static class SingleRecordInputFormat extends InputFormat {
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      return Collections.<InputSplit>singletonList(new SingleRecordSplit());
    }
    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new RecordReader<Void, Void>() {
        private int count = 0;
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        }
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
          return count++ == 0;
        }
        @Override
        public Void getCurrentKey() throws IOException, InterruptedException {
          return null;
        }
        @Override
        public Void getCurrentValue() throws IOException, InterruptedException {
          return null;
        }
        @Override
        public float getProgress() throws IOException, InterruptedException {
          return count;
        }
        @Override
        public void close() throws IOException {
        }
      };
    }
  }

  private static class NoOpOutputFormat extends OutputFormat<Void, Void> {
    @Override
    public RecordWriter<Void, Void> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new RecordWriter<Void, Void>() {
        @Override
        public void write(Void key, Void value) throws IOException, InterruptedException {
        }
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        }
      };
    }
    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    }
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new OutputCommitter() {
        @Override
        public void setupJob(JobContext jobContext) throws IOException {
        }
        @Override
        public void setupTask(TaskAttemptContext taskContext) throws IOException {
        }
        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
          return false;
        }
        @Override
        public void commitTask(TaskAttemptContext taskContext) throws IOException {
        }
        @Override
        public void abortTask(TaskAttemptContext taskContext) throws IOException {
        }
      };
    }
  }

  public static class TxSpark extends AbstractSpark implements JavaSparkMain {
    @Override
    protected void configure() {
      setName(SPARK_TX);
      setMainClass(TxSpark.class);
    }

    @Override
    protected void initialize() throws Exception {
      // this job will fail because we don't configure the mapper etc. That is fine because destroy() still gets called
      recordTransaction(getContext(), SPARK_TX, INITIALIZE);
      attemptNestedTransaction(getContext(), SPARK_TX, INITIALIZE_NEST);
    }

    @Override
    public void destroy() {
      recordTransaction(getContext(), SPARK_TX, DESTROY);
      attemptNestedTransaction(getContext(), SPARK_TX, DESTROY_NEST);
    }

    @Override
    public void run(JavaSparkExecutionContext sec) throws Exception {
      // no-op
    }
  }

  public static class NoTxSpark extends TxSpark {
    @Override
    protected void configure() {
      super.configure();
      setName(SPARK_NOTX);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    protected void initialize() throws Exception {
      // this job will fail because we don't configure the mapper etc. That is fine because destroy() still gets called
      recordTransaction(getContext(), SPARK_NOTX, INITIALIZE);
      executeRecordTransaction(getContext(), SPARK_NOTX,
                               INITIALIZE_TX, TIMEOUT_SPARK_INITIALIZE);
      executeAttemptNestedTransaction(getContext(),
                                      SPARK_NOTX, INITIALIZE_NEST);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void destroy() {
      recordTransaction(getContext(), SPARK_NOTX, DESTROY);
      executeRecordTransaction(getContext(), SPARK_NOTX,
                               DESTROY_TX, TIMEOUT_SPARK_DESTROY);
      executeAttemptNestedTransaction(getContext(),
                                      SPARK_NOTX, DESTROY_NEST);
    }
  }
}
