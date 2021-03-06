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

package io.cdap.cdap.security.auth.context;

import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;

import java.util.Properties;

/**
 * A dummy {@link AuthenticationContext} to be used in tests.
 */
public class AuthenticationTestContext implements AuthenticationContext {
  @Override
  public Principal getPrincipal() {
    Properties properties = System.getProperties();
    String credentialValue = properties.getProperty("user.credential.value");
    String credentialTypeStr = properties.getProperty("user.credential.type");
    Credential credential = null;
    if (credentialValue != null && credentialTypeStr != null) {
      Credential.CredentialType credentialType = Credential.CredentialType
        .valueOf(credentialTypeStr);
      credential = new Credential(credentialValue, credentialType);
    }
    return new Principal(System.getProperty("user.name"), Principal.PrincipalType.USER, credential);
  }
}
