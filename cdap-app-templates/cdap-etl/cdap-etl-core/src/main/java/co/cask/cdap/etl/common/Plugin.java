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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.artifact.ArtifactId;
import com.google.common.base.Objects;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Plugin Configuration that is part of {@link ETLStage}
 */
public class Plugin {
  private final String name;
  private final Map<String, String> properties;
  private final ArtifactId artifactId;

  public Plugin(String name, Map<String, String> properties, @Nullable ArtifactId artifactId) {
    this.name = name;
    this.properties = properties;
    this.artifactId = artifactId;
  }

  public Plugin(String name, Map<String, String> properties) {
    this(name, properties, null);
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Nullable
  public ArtifactId getArtifactId() {
    return artifactId;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("properties", properties)
      .add("artifact", artifactId)
      .toString();
  }

}
