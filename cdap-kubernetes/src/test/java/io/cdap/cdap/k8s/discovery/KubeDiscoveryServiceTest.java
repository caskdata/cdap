/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.k8s.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.util.Config;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Test for {@link KubeDiscoveryService}. The test is disabled by default since it requires a running
 * kubernetes cluster for the test to run. This class is kept for development purpose.
 */
//@Ignore
public class KubeDiscoveryServiceTest {

  @Test
  public void testDiscoveryService() throws Exception {
    Map<String, String> podLabels = ImmutableMap.of("cdap.container", "test");
    try (KubeDiscoveryService service = new KubeDiscoveryService("default", "cdap-test-",
                                                                 podLabels, Collections.emptyList())) {
      // Watch for changes
      ServiceDiscovered serviceDiscovered = service.discover("test.service");

      BlockingQueue<Set<Discoverable>> queue = new LinkedBlockingQueue<>();
      serviceDiscovered.watchChanges(sd -> {
        // Update to the latest
        queue.add(StreamSupport.stream(sd.spliterator(), false).collect(Collectors.toSet()));
      }, MoreExecutors.directExecutor());

      // Initially there should be no such service
      Set<Discoverable> discoverables = queue.poll(5, TimeUnit.SECONDS);
      Assert.assertNotNull(discoverables);
      Assert.assertTrue(discoverables.isEmpty());

      // Then, register the service
      service.register(new Discoverable("test.service",
                                        new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234),
                                        "https".getBytes(StandardCharsets.UTF_8)));

      // Now there should be a new discoverable
      discoverables = queue.poll(5, TimeUnit.SECONDS);
      Assert.assertNotNull(discoverables);
      Assert.assertEquals(1, discoverables.size());

      // The discoverable should have hostname the same as the k8s service name and port from the discoverable
      Discoverable discoverable = discoverables.stream().findFirst().orElseThrow(Exception::new);
      Assert.assertEquals(1234, discoverable.getSocketAddress().getPort());
      Assert.assertEquals("https", new String(discoverable.getPayload(), StandardCharsets.UTF_8));
      Assert.assertEquals("cdap-test-test-service", discoverable.getSocketAddress().getHostName());

      // Register the service again with different port. This is to simulate config update
      service.register(new Discoverable("test.service", new InetSocketAddress(InetAddress.getLoopbackAddress(), 4321)));

      // Should have a new discoverable
      discoverables = queue.poll(5, TimeUnit.SECONDS);
      Assert.assertNotNull(discoverables);
      Assert.assertEquals(1, discoverables.size());

      // The discoverable should have hostname the same as the k8s service name and port from the discoverable
      discoverable = discoverables.stream().findFirst().orElseThrow(Exception::new);
      Assert.assertEquals(4321, discoverable.getSocketAddress().getPort());
      Assert.assertArrayEquals(new byte[0], discoverable.getPayload());
      Assert.assertEquals("cdap-test-test-service", discoverable.getSocketAddress().getHostName());
    } finally {
      // Cleanup the created service
      CoreV1Api api = new CoreV1Api(Config.defaultClient());
      V1DeleteOptions deleteOptions = new V1DeleteOptions();
      api.deleteNamespacedService("cdap-test-test-service", "default", null, deleteOptions, null, null, null, null);
    }
  }
}
