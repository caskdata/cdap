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

package co.cask.cdap.api.common;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Enum for the different types of scopes.
 */
public enum Scope {
  DATASET("dataset", true),
  MAPREDUCE("mapreduce", false),
  SPARK("spark", false),
  CUSTOM_ACTION("custom_action", false);

  private final String displayName;
  private final boolean retainScope;

  /**
   * Private constructor to force using the enum values.
   */
  private Scope(String name, boolean retainScope) {
    this.displayName = name;
    this.retainScope = retainScope;
  }

  @Override
  public String toString() {
    return displayName;
  }

  public boolean retainableScope() {
    return this.retainScope;
  }

  // helper map for efficient implementation of scopeFor()
  private static final Map<String, Scope> LOOKUP_BY_DISPLAY_NAME;
  static {
    LOOKUP_BY_DISPLAY_NAME = Maps.newHashMapWithExpectedSize(Scope.values().length);
    for (Scope scope : Scope.values()) {
      LOOKUP_BY_DISPLAY_NAME.put(scope.toString(), scope);
    }
  }

  /**
   * @return the Scope represented by a display name.
   */
  public static Scope scopeFor(String displayName) {
    Scope scope = LOOKUP_BY_DISPLAY_NAME.get(displayName);
    if (scope != null) {
      return scope;
    }
    throw new IllegalArgumentException("Illegal scope name: " + displayName);
  }
}
