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

package co.cask.cdap.common;

import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;

/**
 * Thrown when a user is not authorized to perform an operation.
 */
public class UnauthorizedException extends Exception {

  private final boolean authenticationError;

  public UnauthorizedException() {
    super();
    this.authenticationError = true;
  }

  public UnauthorizedException(String msg, Throwable throwable) {
    super(msg, throwable);
    this.authenticationError = true;
  }

  public UnauthorizedException(String message) {
    super(message);
    this.authenticationError = true;
  }

  public UnauthorizedException(Principal principal, Action action, EntityId entityId) {
    super(String.format("Principal '%s' is not authorized to perform action '%s' on entity '%s'",
                        principal, action, entityId));
    this.authenticationError = false;
  }

  public boolean isAuthenticationError() {
    return authenticationError;
  }
}
