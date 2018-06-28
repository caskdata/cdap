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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.WordCountApp;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.gateway.handlers.PreferencesHttpHandler;
import co.cask.cdap.internal.app.deploy.Specifications;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link PreferencesHttpHandler}
 */
public class PreferencesHttpHandlerTest extends AppFabricTestBase {

  private static Store store;

  @BeforeClass
  public static void init() {
    store = getInjector().getInstance(Store.class);
  }

  private void addApplication(String namespace, Application app) {
    ApplicationSpecification appSpec = Specifications.from(app);
    store.addApplication(new ApplicationId(namespace, appSpec.getName()), appSpec);
  }

  @Test
  public void testInstance() throws Exception {
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(), true, 200));
    propMap.put("k1", "3@#3");
    propMap.put("@#$#ljfds", "231@#$");
    setPreferences(getPreferenceURI(), propMap, 200);
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(), true, 200));
    propMap.clear();
    deletePreferences(getPreferenceURI(), 200);
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(), true, 200));
  }

  @Test
  public void testNamespace() throws Exception {
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(TEST_NAMESPACE1), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(TEST_NAMESPACE2), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(TEST_NAMESPACE1), true, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(TEST_NAMESPACE2), true, 200));
    propMap.put("k1", "3@#3");
    propMap.put("@#$#ljfds", "231@#$");
    setPreferences(getPreferenceURI(TEST_NAMESPACE1), propMap, 200);
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(TEST_NAMESPACE1), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(TEST_NAMESPACE1), true, 200));

    Map<String, String> instanceMap = Maps.newHashMap();
    instanceMap.put("k1", "432432*#######");
    setPreferences(getPreferenceURI(), instanceMap, 200);
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceURI(), true, 200));
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceURI(TEST_NAMESPACE2), true, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(TEST_NAMESPACE1), true, 200));

    instanceMap.put("k2", "(93424");
    setPreferences(getPreferenceURI(), instanceMap, 200);
    instanceMap.putAll(propMap);
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceURI(TEST_NAMESPACE1), true, 200));

    deletePreferences(getPreferenceURI(TEST_NAMESPACE1), 200);
    deletePreferences(getPreferenceURI(TEST_NAMESPACE2), 200);

    instanceMap.clear();
    instanceMap.put("*&$kjh", "*(&*1");
    setPreferences(getPreferenceURI(), instanceMap, 200);
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceURI(TEST_NAMESPACE2), true, 200));
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceURI(TEST_NAMESPACE1), true, 200));
    instanceMap.clear();
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceURI(TEST_NAMESPACE2), false, 200));
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceURI(TEST_NAMESPACE1), false, 200));

    deletePreferences(getPreferenceURI(), 200);
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceURI(TEST_NAMESPACE2), true, 200));
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceURI(TEST_NAMESPACE1), true, 200));
    getPreferences(getPreferenceURI("invalidNamespace"), true, 404);
  }

  @Test
  public void testApplication() throws Exception {
    addApplication(TEST_NAMESPACE1, new WordCountApp());
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(TEST_NAMESPACE1, "WordCountApp"), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(TEST_NAMESPACE1, "WordCountApp"), true, 200));
    getPreferences(getPreferenceURI(TEST_NAMESPACE1, "InvalidAppName"), false, 404);
    setPreferences(getPreferenceURI(), ImmutableMap.of("k1", "instance"), 200);
    setPreferences(getPreferenceURI(TEST_NAMESPACE1), ImmutableMap.of("k1", "namespace"), 200);
    setPreferences(getPreferenceURI(TEST_NAMESPACE1, "WordCountApp"), ImmutableMap.of("k1", "application"), 200);
    Assert.assertEquals("application",
                        getPreferences(getPreferenceURI(TEST_NAMESPACE1, "WordCountApp"), false, 200).get("k1"));
    Assert.assertEquals("application",
                        getPreferences(getPreferenceURI(TEST_NAMESPACE1, "WordCountApp"), true, 200).get("k1"));
    Assert.assertEquals("namespace", getPreferences(getPreferenceURI(TEST_NAMESPACE1), false, 200).get("k1"));
    Assert.assertEquals("namespace", getPreferences(getPreferenceURI(TEST_NAMESPACE1), true, 200).get("k1"));
    Assert.assertEquals("instance", getPreferences(getPreferenceURI(), true, 200).get("k1"));
    Assert.assertEquals("instance", getPreferences(getPreferenceURI(), false, 200).get("k1"));
    deletePreferences(getPreferenceURI(TEST_NAMESPACE1, "WordCountApp"), 200);
    Assert.assertEquals("namespace",
                        getPreferences(getPreferenceURI(TEST_NAMESPACE1, "WordCountApp"), true, 200).get("k1"));
    Assert.assertNull(getPreferences(getPreferenceURI(TEST_NAMESPACE1, "WordCountApp"), false, 200).get("k1"));
    deletePreferences(getPreferenceURI(TEST_NAMESPACE1), 200);
    Assert.assertEquals("instance",
                        getPreferences(getPreferenceURI(TEST_NAMESPACE1, "WordCountApp"), true, 200).get("k1"));
    Assert.assertEquals("instance",
                        getPreferences(getPreferenceURI(TEST_NAMESPACE1), true, 200).get("k1"));
    Assert.assertNull(getPreferences(getPreferenceURI(TEST_NAMESPACE1), false, 200).get("k1"));
    deletePreferences(getPreferenceURI(), 200);
    Assert.assertNull(getPreferences(getPreferenceURI(), true, 200).get("k1"));
    Assert.assertNull(getPreferences(getPreferenceURI(TEST_NAMESPACE1), true, 200).get("k1"));
    Assert.assertNull(getPreferences(getPreferenceURI(TEST_NAMESPACE1, "WordCountApp"), true, 200).get("k1"));
  }

  @Test
  public void testProgram() throws Exception {
    addApplication(TEST_NAMESPACE2, new WordCountApp());
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getPreferences(
      getPreferenceURI(TEST_NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"), false, 200));
    getPreferences(getPreferenceURI(TEST_NAMESPACE2, "WordCountApp", "invalidType", "somename"), false, 400);
    getPreferences(getPreferenceURI(TEST_NAMESPACE2, "WordCountApp", "flows", "somename"), false, 404);
    propMap.put("k1", "k349*&#$");
    setPreferences(getPreferenceURI(TEST_NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"), propMap, 200);
    Assert.assertEquals(propMap, getPreferences(
      getPreferenceURI(TEST_NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"), false, 200));
    propMap.put("k1", "instance");
    setPreferences(getPreferenceURI(), propMap, 200);
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(), true, 200));
    propMap.put("k1", "k349*&#$");
    Assert.assertEquals(propMap, getPreferences(
      getPreferenceURI(TEST_NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"), false, 200));
    deletePreferences(getPreferenceURI(TEST_NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"), 200);
    propMap.put("k1", "instance");
    Assert.assertEquals(0, getPreferences(getPreferenceURI(TEST_NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"),
                                          false, 200).size());
    Assert.assertEquals(propMap, getPreferences(
      getPreferenceURI(TEST_NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"), true, 200));
    deletePreferences(getPreferenceURI(), 200);
    propMap.clear();
    Assert.assertEquals(propMap, getPreferences(
      getPreferenceURI(TEST_NAMESPACE2, "WordCountApp", "flows", "WordCountFlow"), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceURI(), false, 200));
  }

  @Test
  public void testSetPreferenceWithProfiles() throws Exception {
    // put my profile
    ProfileId myProfile = new ProfileId(TEST_NAMESPACE1, "MyProfile");
    putProfile(myProfile, Profile.NATIVE, 200);

    // put some properties with my profile, it should work fine
    Map<String, String> properties = new HashMap<>();
    properties.put("1st key", "1st value");
    properties.put("2nd key", "2nd value");
    properties.put(SystemArguments.PROFILE_NAME, "USER:MyProfile");
    Map<String, String> expected = ImmutableMap.copyOf(properties);
    setPreferences(getPreferenceURI(TEST_NAMESPACE1), properties, 200);
    Assert.assertEquals(expected, getPreferences(getPreferenceURI(TEST_NAMESPACE1), false, 200));

    // put some property with non-existing profile, it should fail with 404
    properties.put(SystemArguments.PROFILE_NAME, "NonExisting");
    setPreferences(getPreferenceURI(TEST_NAMESPACE1), properties, 404);
    Assert.assertEquals(expected, getPreferences(getPreferenceURI(TEST_NAMESPACE1), false, 200));

    // disable the profile and put again, it should fail with 409
    disableProfile(myProfile, 200);
    properties.put(SystemArguments.PROFILE_NAME, "USER:MyProfile");
    setPreferences(getPreferenceURI(TEST_NAMESPACE1), properties, 409);
    Assert.assertEquals(expected, getPreferences(getPreferenceURI(TEST_NAMESPACE1), false, 200));

    deletePreferences(getPreferenceURI(TEST_NAMESPACE1), 200);
  }
}
