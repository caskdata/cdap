/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.security;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Responsible for executing code for a user, configurable at the namespace level.
 */
public class Impersonator {
  private final boolean kerberosEnabled;
  private final UGIProvider ugiProvider;
  private final ImpersonationUserResolver impersonationUserResolver;

  @Inject
  @VisibleForTesting
  public Impersonator(CConfiguration cConf, UGIProvider ugiProvider,
                      ImpersonationUserResolver impersonationUserResolver) {
    this.kerberosEnabled = SecurityUtil.isKerberosEnabled(cConf);
    this.ugiProvider = ugiProvider;
    this.impersonationUserResolver = impersonationUserResolver;
  }

  /**
   * Executes a callable as the user, configurable at a namespace level
   *
   * @param namespaceId the namespace to use to lookup the user
   * @param callable the callable to execute
   * @param <T> return type of the callable
   *
   * @return the return value of the callable
   * @throws Exception if the callable throws any exception
   */
  public <T> T doAs(NamespaceId namespaceId, final Callable<T> callable) throws Exception {
    if (!kerberosEnabled) {
      return callable.call();
    }
    return ImpersonationUtils.doAs(getUGI(namespaceId), callable);
  }

  public UserGroupInformation getUGI(NamespaceId namespaceId) throws IOException {
    if (!kerberosEnabled) {
      return UserGroupInformation.getCurrentUser();
    }

    ImpersonationInfo impersonationInfo = impersonationUserResolver.getImpersonationInfo(namespaceId);
    return ugiProvider.getConfiguredUGI(impersonationInfo);
  }
}
