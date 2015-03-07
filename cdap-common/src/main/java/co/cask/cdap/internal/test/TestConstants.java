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

package co.cask.cdap.internal.test;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;

/**
 * Common tests constants for CDAP tests.
 */
public class TestConstants {
  public static final Id.Namespace TEST_NAMESPACE = Id.Namespace.from("mycdaphandlertestspace");
  public static final NamespaceMeta TEST_NAMESPACE_META = new NamespaceMeta.Builder()
    .setId("mycdaphandlertestspace").build();
  public static final String URL_PREFIX = "/v3/namespaces/" + TEST_NAMESPACE.getId();
}
