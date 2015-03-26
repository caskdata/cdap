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

package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * An Abstract class implementing {@link ProgramWorkflowRunner}, providing a {@link Callable} of
 * {@link RuntimeContext} to programs running in a workflow.
 * <p>
 * Programs that extend this class (such as {@link MapReduceProgramWorkflowRunner} or
 * {@link SparkProgramWorkflowRunner}) can execute their associated programs through the
 * {@link AbstractProgramWorkflowRunner#executeProgram} by providing the {@link ProgramController} and
 * {@link RuntimeContext} that they obtained through the {@link ProgramRunner}.
 * </p>
 * The {@link RuntimeContext} is blocked until completion of the associated program.
 */
public abstract class AbstractProgramWorkflowRunner implements ProgramWorkflowRunner {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowDriver.class);

  protected final WorkflowSpecification workflowSpec;
  protected final ProgramRunnerFactory programRunnerFactory;
  protected final Program workflowProgram;
  private final RunId runId;
  private final Arguments userArguments;
  private final long logicalStartTime;
  private final Store store;

  public AbstractProgramWorkflowRunner(Arguments runtimeArguments, RunId runId, Program workflowProgram,
                                       long logicalStartTime, ProgramRunnerFactory programRunnerFactory,
                                       WorkflowSpecification workflowSpec, Store store) {
    this.userArguments = runtimeArguments;
    this.runId = runId;
    this.workflowProgram = workflowProgram;
    this.logicalStartTime = logicalStartTime;
    this.programRunnerFactory = programRunnerFactory;
    this.workflowSpec = workflowSpec;
    this.store = store;
  }

  @Override
  public abstract Callable<RuntimeContext> create(String name);

  @Override
  public abstract RuntimeContext runAndWait(Program program, ProgramOptions options) throws Exception;

  /**
   * Gets a {@link Callable} of {@link RuntimeContext} for the {@link Program}.
   *
   * @param name    name of the {@link Program}
   * @param program the {@link Program}
   * @return a {@link Callable} of {@link RuntimeContext} for this {@link Program}
   */
  protected Callable<RuntimeContext> getRuntimeContextCallable(String name, final Program program) {
    final ProgramOptions options = new SimpleProgramOptions(
      program.getName(),
      new BasicArguments(ImmutableMap.of(
        ProgramOptionConstants.RUN_ID, runId.getId(),
        ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(logicalStartTime),
        ProgramOptionConstants.WORKFLOW_BATCH, name
      )),
      new BasicArguments(RuntimeArguments.extractScope(Scope.scopeFor(program.getType().getCategoryName()), name,
                                                       userArguments.asMap()))
    );

    return new Callable<RuntimeContext>() {
      @Override
      public RuntimeContext call() throws Exception {
        return runAndWait(program, options);
      }
    };
  }


  /**
   * Adds a listener to the {@link ProgramController} and blocks for completion.
   *
   *
   * @param programId the {@link Id.Program} of the program to be run
   * @param controller the {@link ProgramController} for the program
   * @param context    the {@link RuntimeContext}
   * @return {@link RuntimeContext} of the completed program
   * @throws Exception if the execution failed
   */
  protected RuntimeContext executeProgram(final Id.Program programId, final ProgramController controller,
                                          final RuntimeContext context) throws Exception {
    // TODO: Perhaps generate a different runId for each program contained in a workflow
    final String runId = controller.getRunId().getId();
    // Execute the program.
    final SettableFuture<RuntimeContext> completion = SettableFuture.create();
    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State state, @Nullable Throwable cause) {
        store.setStart(programId, runId, TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
        if (state == ProgramController.State.COMPLETED) {
          completed();
        }
        if (state == ProgramController.State.ERROR) {
          error(controller.getFailureCause());
        }
      }

      @Override
      public void completed() {
        store.setStop(programId, runId,
                      TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                      ProgramController.State.COMPLETED.getRunStatus());
        completion.set(context);
      }

      @Override
      public void killed() {
        store.setStop(programId, runId,
                      TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                      ProgramController.State.KILLED.getRunStatus());
        completion.set(context);
      }

      @Override
      public void error(Throwable cause) {
        LOG.info("Program stopped with error {}, {}", programId, runId, cause);
        store.setStop(programId, runId,
                      TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                      ProgramController.State.ERROR.getRunStatus());
        completion.setException(cause);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    // Block for completion.
    try {
      return completion.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw Throwables.propagate(cause);
    } catch (InterruptedException e) {
      try {
        Futures.getUnchecked(controller.stop());
      } catch (Throwable t) {
        // no-op
      }
      // reset the interrupt
      Thread.currentThread().interrupt();
      return null;
    }
  }
}
