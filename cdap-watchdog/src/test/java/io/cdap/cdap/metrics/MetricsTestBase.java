/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package io.cdap.cdap.metrics;

import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.io.DatumWriter;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.internal.io.ASMDatumWriterFactory;
import io.cdap.cdap.internal.io.ASMFieldAccessorFactory;
import io.cdap.cdap.internal.io.ReflectionSchemaGenerator;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.metrics.process.MessagingMetricsProcessorManagerService;
import io.cdap.cdap.metrics.process.loader.MockMetricsWriterModule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * TestBase for testing {@link io.cdap.cdap.metrics.collect.MessagingMetricsCollectionService} 
 * and {@link MessagingMetricsProcessorManagerService}
 */
public abstract class MetricsTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  protected static Injector injector;
  protected static final String TOPIC_PREFIX = "metrics";

  protected static CConfiguration cConf;
  protected static MessagingService messagingService;
  protected static TypeToken<MetricValues> metricValueType;
  protected static Schema schema;
  protected static DatumWriter<MetricValues> recordWriter;

  @Before
  public void init() throws IOException, UnsupportedTypeException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.Metrics.TOPIC_PREFIX, TOPIC_PREFIX);
    cConf.setInt(Constants.Metrics.MESSAGING_TOPIC_NUM, 10);
    cConf.setInt(Constants.Metrics.QUEUE_SIZE, 1000);
    // Set it to really short delay for faster test
    cConf.setLong(Constants.Metrics.PROCESSOR_MAX_DELAY_MS, 5);

    injector = Guice.createInjector(getModules());
    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    metricValueType = TypeToken.of(MetricValues.class);
    schema = new ReflectionSchemaGenerator().generate(metricValueType.getType());
    recordWriter = new ASMDatumWriterFactory(new ASMFieldAccessorFactory())
      .create(metricValueType, schema);
  }

  @After
  public void stop() {
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  private List<Module> getModules() {
    List<Module> modules = new ArrayList<>();
    modules.add(new ConfigModule(cConf));
    modules.add(new InMemoryDiscoveryModule());
    modules.add(new MessagingServerRuntimeModule().getInMemoryModules());
    modules.add(new MockMetricsWriterModule());
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsCollectionService.class).toInstance(new NoOpMetricsCollectionService());
      }
    });
    modules.addAll(getAdditionalModules());
    return modules;
  }

  protected abstract List<Module> getAdditionalModules();
}
