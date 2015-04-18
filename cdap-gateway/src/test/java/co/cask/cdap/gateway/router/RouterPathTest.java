/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.NoAuthenticator;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *  To test the RouterPathLookup regular expression tests.
 */
public class RouterPathTest {

  private static RouterPathLookup pathLookup;
  private static final HttpVersion VERSION = HttpVersion.HTTP_1_1;
  private static final String API_KEY = "SampleTestApiKey";
  private static final String FALLBACKSERVICE = "gateway";

  @BeforeClass
  public static void init() throws Exception {
    pathLookup = new RouterPathLookup(new NoAuthenticator());
  }

  @Test
  public void testSystemServicePath() {
    String path = "/v2/system/services/foo/logs";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);

    path = "/v3/system/services/foo/logs";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);

    // this clashes with a rule for stream handler and fails if the rules are evaluated in wrong order [CDAP-2159]
    path = "/v3/system/services/streams/logs";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);
  }

  @Test
  public void testMetricsPath() throws Exception {
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
    String flowPath = "/v2///metrics/system/apps/InvalidApp//";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);

    flowPath = "/v2/metrics";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);

    flowPath = "/v2/metrics//";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);

    testMetricsPath("/v3/metrics/search?target=childContext&context=user");
    testMetricsPath("/v3/metrics/search?target=childContext&context=PurchaeHistory.f.PurchaseFlow");
    testMetricsPath("/v3/metrics/search?target=metric&context=PurchaeHistory.f.PurchaseFlow");
  }

  private void testMetricsPath(String path) {
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);
  }

  @Test
  public void testAppFabricPath() throws Exception {
    //Default destination for URIs will APP_FABRIC_HTTP
    String flowPath = "/v2/ping/";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    flowPath = "/status";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    flowPath = "/v2/monitor///abcd/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testLogPath() throws Exception {
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
    String flowPath = "/v3/namespaces/default/apps//InvalidApp///flows/FlowName/logs/";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);

    flowPath = "///v3/namespaces/default///apps/InvalidApp/flows/FlowName/////logs";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);

    flowPath = "/v3/namespaces/default/apps/InvalidApp/service/ServiceName/runs/7e6adc79-0f5d-4252-70817ea47698/logs/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);

    flowPath = "/v3/namespaces/default/apps/InvalidApp/adapters/Adapter1/runs/7e6adc79-0f5d-b559-70817ea47698/logs/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);
  }

  @Test
  public void testServicePath() throws Exception {
    // Service Path "/v2/apps/{app-id}/services/{service-id}/methods/<user-defined-method-path>"
    // Discoverable Service Name -> "service.%s.%s.%s", namespaceId, appId, serviceId

    String servicePath = "/v2/apps//PurchaseHistory///services/CatalogLookup///methods//ping/1";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals("service." + Constants.DEFAULT_NAMESPACE + ".PurchaseHistory.CatalogLookup", result);

    servicePath = "///v2///apps/PurchaseHistory-123//services/weird!service@@NAme///methods/echo/someParam";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals("service." + Constants.DEFAULT_NAMESPACE + ".PurchaseHistory-123.weird!service@@NAme", result);

    servicePath = "v2/apps/SomeApp_Name/services/CatalogLookup/methods/getHistory/itemID";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals("service." + Constants.DEFAULT_NAMESPACE + ".SomeApp_Name.CatalogLookup", result);

    // The following two should resort to resort to APP_FABRIC_HTTP, because there is no actual method being called.
    servicePath = "v2/apps/AppName/services/CatalogLookup//methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    servicePath = "v2/apps/otherAppName/services/CatalogLookup//methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    // v3 servicePaths
    servicePath = "/v3/namespaces/testnamespace/apps//PurchaseHistory///services/CatalogLookup///methods//ping/1";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals("service.testnamespace.PurchaseHistory.CatalogLookup", result);

    servicePath = "///v3/namespaces/testnamespace//apps/PurchaseHistory-123//services/weird!service@@NAme///methods/" +
      "echo/someParam";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals("service.testnamespace.PurchaseHistory-123.weird!service@@NAme", result);

    servicePath = "v3/namespaces/testnamespace/apps/SomeApp_Name/services/CatalogLookup/methods/getHistory/itemID";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals("service.testnamespace.SomeApp_Name.CatalogLookup", result);

    servicePath = "v3/namespaces/testnamespace/apps/AppName/services/CatalogLookup//methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    servicePath = "v3/namespaces/testnamespace/apps/AppName/services/CatalogLookup////methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testStreamPath() throws Exception {
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
    String streamPath = "/v2/streams";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    streamPath = "///v2/streams///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    streamPath = "v2///streams///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    streamPath = "//v2///streams/HelloStream//flows///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    streamPath = "//v2///streams/HelloStream//flows///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);

    streamPath = "//v2///streams/HelloStream//flows///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);

    streamPath = "v2//streams//flows///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);

    streamPath = "v2//streams/InvalidStreamName/flows/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    streamPath = "v2//streams/InvalidStreamName/flows/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);

    streamPath = "v2//streams/InvalidStreamName/info/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);

    // v3 routes
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
    streamPath = "/v3/namespaces/default/streams";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    streamPath = "///v3/namespaces/default/streams///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    streamPath = "v3/namespaces/default///streams///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    streamPath = "//v3/namespaces/default///streams/HelloStream//flows///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    streamPath = "//v3/namespaces/default///streams/HelloStream//flows///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);

    streamPath = "//v3/namespaces/default///streams/HelloStream//flows///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);

    streamPath = "v3/namespaces/default//streams//flows///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);

    streamPath = "v3/namespaces/default//streams/InvalidStreamName/flows/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    streamPath = "v3/namespaces/default//streams/InvalidStreamName/flows/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);

    streamPath = "v3/namespaces/default//streams/InvalidStreamName/info/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);
  }

  @Test
  public void testRouterFlowPathLookUp() throws Exception {
    String flowPath = "/v2//apps/ResponseCodeAnalytics/flows/LogAnalyticsFlow/status";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    flowPath = "/v3/namespaces/default//apps/ResponseCodeAnalytics/flows/LogAnalyticsFlow/status";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testRouterWorkFlowPathLookUp() throws Exception {
    String path = "/v2/apps///PurchaseHistory///workflows/PurchaseHistoryWorkflow/status";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);

    path = "/v3/namespaces/default/apps///PurchaseHistory///workflows/PurchaseHistoryWorkflow/status";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterDeployPathLookUp() throws Exception {
    String path = "/v2//apps/";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), path);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);

    path = "/v3/namespaces/default//apps/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), path);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterFlowletInstancesLookUp() throws Exception {
    String path = "/v2//apps/WordCount/flows/WordCountFlow/flowlets/StreamSource/instances";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), path);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);

    path = "/v3/namespaces/default//apps/WordCount/flows/WordCountFlow/flowlets/StreamSource/instances";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), path);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterExplorePathLookUp() throws Exception {
    String explorePath = "/v2//data///explore//datasets////mydataset//enable";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), explorePath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, explorePath, httpRequest);
    Assert.assertEquals(Constants.Service.EXPLORE_HTTP_USER_SERVICE, result);

    explorePath = "/v3/namespaces/default//data///explore//datasets////mydataset//enable";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), explorePath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, explorePath, httpRequest);
    Assert.assertEquals(Constants.Service.EXPLORE_HTTP_USER_SERVICE, result);
  }

  @Test
  public void testRouterWebAppPathLookUp() throws Exception {
    //Calls to webapp service with appName in the first split of URI will be routed to webappService
    //But if it has v2 then use the regular router lookup logic to find the appropriate service
    final String webAppService = "webapp$HOST";
    String procPath = "/sentiApp/abcd/efgh///";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), procPath);
    String result = pathLookup.getRoutingService(webAppService, procPath, httpRequest);
    Assert.assertEquals(webAppService, result);

    procPath = "/v2//metrics///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), procPath);
    result = pathLookup.getRoutingService(webAppService, procPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);

    procPath = "/v3//metrics///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), procPath);
    result = pathLookup.getRoutingService(webAppService, procPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);
  }

  @Test
  public void testRouterV3PathLookup() {
    final String namespacePath = "/v3////namespace/////";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), namespacePath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, namespacePath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testRouterFeedsLookup() {
    final String namespacePath = "/v3//feeds/test";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), namespacePath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, namespacePath, httpRequest);
    Assert.assertEquals(null, result);
  }
}
