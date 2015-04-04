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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.StageSpecification;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Default implementation of {@link StageConfigurer}.
 */
public class DefaultStageConfigurer implements StageConfigurer {
  private final List<Property> properties = Lists.newArrayList();

  private String name;
  private String className;
  private String description;

  public DefaultStageConfigurer(Class klass) {
    this.name = klass.getSimpleName();
    this.className = klass.getName();
    this.description = "";
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void addProperties(List<Property> properties) {
    properties.addAll(properties);
  }

  @Override
  public void addProperty(Property property) {
    properties.add(property);
  }

  public StageSpecification createSpecification() {
    return new StageSpecification(className, name, description, properties);
  }
}
