/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.provision;

import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Information about a provisioning task for a program run.
 */
public class ProvisioningTaskInfo {
  private final ProvisioningOp op;
  private final ProgramRunId programRunId;
  private final ProgramDescriptor programDescriptor;
  private final ProgramOptions programOptions;
  private final Map<String, String> provisionerProperties;
  private final String user;
  private final String provisionerName;
  private final SecureKeyInfo secureKeyInfo;
  private final Cluster cluster;

  public ProvisioningTaskInfo(ProgramRunId programRunId, ProgramDescriptor programDescriptor,
                              ProgramOptions programOptions, Map<String, String> provisionerProperties,
                              String provisionerName, String user, ProvisioningOp op,
                              @Nullable SecureKeyInfo secureKeyInfo, @Nullable Cluster cluster) {
    this.programRunId = programRunId;
    this.provisionerProperties = provisionerProperties;
    this.programDescriptor = programDescriptor;
    this.programOptions = programOptions;
    this.user = user;
    this.provisionerName = provisionerName;
    this.op = op;
    this.secureKeyInfo = secureKeyInfo;
    this.cluster = cluster;
  }

  public ProvisioningTaskInfo(ProvisioningTaskInfo existing, ProvisioningOp op, @Nullable Cluster cluster) {
    this(existing.getProgramRunId(), existing.getProgramDescriptor(), existing.getProgramOptions(),
         existing.getProvisionerProperties(), existing.getProvisionerName(), existing.getUser(), op,
         existing.getSecureKeyInfo(), cluster);
  }

  public ProvisioningTaskKey getTaskKey() {
    return new ProvisioningTaskKey(programRunId, op.getType());
  }

  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  public ProgramDescriptor getProgramDescriptor() {
    return programDescriptor;
  }

  public ProgramOptions getProgramOptions() {
    return programOptions;
  }

  public Map<String, String> getProvisionerProperties() {
    return provisionerProperties;
  }

  public String getUser() {
    return user;
  }

  public String getProvisionerName() {
    return provisionerName;
  }

  public ProvisioningOp getProvisioningOp() {
    return op;
  }

  @Nullable
  public SecureKeyInfo getSecureKeyInfo() {
    return secureKeyInfo;
  }

  @Nullable
  public Cluster getCluster() {
    return cluster;
  }

}
