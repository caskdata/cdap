/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.MethodNotAllowedException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.profile.ProfileCreateRequest;
import io.cdap.cdap.proto.provisioner.ProvisionerInfo;
import io.cdap.cdap.proto.provisioner.ProvisionerPropertyValue;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * {@link io.cdap.http.HttpHandler} for managing profiles.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class ProfileHttpHandler extends AbstractHttpHandler {
  private static final Gson GSON = new GsonBuilder().create();

  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final ProfileService profileService;
  private final ProvisioningService provisioningService;

  @Inject
  public ProfileHttpHandler(AccessEnforcer accessEnforcer,
                            AuthenticationContext authenticationContext,
                            ProfileService profileService,
                            ProvisioningService provisioningService) {
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.profileService = profileService;
    this.provisioningService = provisioningService;
  }

  @GET
  @Path("/profiles")
  public void getSystemProfiles(HttpRequest request, HttpResponder responder) throws Exception {
    NamespaceId namespaceId = NamespaceId.SYSTEM;
    accessEnforcer.enforceOnParent(EntityType.PROFILE, namespaceId, authenticationContext.getPrincipal(),
                                   StandardPermission.LIST);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(profileService.getProfiles(namespaceId, true)));
  }

  @GET
  @Path("/profiles/{profile-name}")
  public void getSystemProfile(HttpRequest request, HttpResponder responder,
                               @PathParam("profile-name") String profileName) throws Exception {
    ProfileId profileId = getValidatedProfile(NamespaceId.SYSTEM, profileName);
    accessEnforcer.enforce(profileId, authenticationContext.getPrincipal(), StandardPermission.GET);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(profileService.getProfile(profileId)));
  }

  @PUT
  @Path("/profiles/{profile-name}")
  public void writeSystemProfile(FullHttpRequest request, HttpResponder responder,
                                 @PathParam("profile-name") String profileName) throws Exception {
    ProfileId profileId = getValidatedProfile(NamespaceId.SYSTEM, profileName);
    checkPutProfilePermissions(profileId);
    writeProfile(profileId, request);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/profiles/{profile-name}/enable")
  public void enableSystemProfile(HttpRequest request, HttpResponder responder,
                                  @PathParam("profile-name") String profileName) throws Exception {
    ProfileId profileId = getValidatedProfile(NamespaceId.SYSTEM, profileName);
    accessEnforcer.enforce(profileId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    profileService.enableProfile(profileId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/profiles/{profile-name}/disable")
  public void disableSystemProfile(HttpRequest request, HttpResponder responder,
                                   @PathParam("profile-name") String profileName) throws Exception {
    ProfileId profileId = getValidatedProfile(NamespaceId.SYSTEM, profileName);
    accessEnforcer.enforce(profileId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    profileService.disableProfile(getValidatedProfile(NamespaceId.SYSTEM, profileName));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @DELETE
  @Path("/profiles/{profile-name}")
  public void deleteSystemProfile(HttpRequest request, HttpResponder responder,
                                  @PathParam("profile-name") String profileName) throws Exception {
    ProfileId profileId = getValidatedProfile(NamespaceId.SYSTEM, profileName);
    accessEnforcer.enforce(profileId, authenticationContext.getPrincipal(), StandardPermission.DELETE);
    profileService.deleteProfile(getValidatedProfile(NamespaceId.SYSTEM, profileName));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * List the profiles in the given namespace. By default the results will not contain profiles in system scope.
   */
  @GET
  @Path("/namespaces/{namespace-id}/profiles")
  public void getProfiles(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @QueryParam("includeSystem") @DefaultValue("false") String includeSystem) throws Exception {
    NamespaceId namespace = getValidatedNamespace(namespaceId);
    accessEnforcer.enforceOnParent(EntityType.PROFILE, namespace, authenticationContext.getPrincipal(),
                                   StandardPermission.LIST);
    boolean include = Boolean.valueOf(includeSystem);
    if (include) {
      accessEnforcer.enforceOnParent(EntityType.PROFILE, NamespaceId.SYSTEM, authenticationContext.getPrincipal(),
                                     StandardPermission.LIST);
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(profileService.getProfiles(namespace, include)));
  }

  /**
   * Get the information about a specific profile.
   */
  @GET
  @Path("/namespaces/{namespace-id}/profiles/{profile-name}")
  public void getProfile(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("profile-name") String profileName) throws Exception {
    ProfileId profileId = getValidatedProfile(namespaceId, profileName);
    accessEnforcer.enforce(profileId, authenticationContext.getPrincipal(), StandardPermission.GET);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(profileService.getProfile(profileId)));
  }

  /**
   * Write a profile in a namespace.
   */
  @PUT
  @Path("/namespaces/{namespace-id}/profiles/{profile-name}")
  public void writeProfile(FullHttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId,
                           @PathParam("profile-name") String profileName) throws Exception {
    ProfileId profileId = getValidatedProfile(namespaceId, profileName);
    checkPutProfilePermissions(profileId);
    writeProfile(profileId, request);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Delete a profile from a namespace. A profile must be in the disabled state before it can be deleted.
   * Before a profile can be deleted, it cannot be assigned to any program or schedule,
   * and it cannot be in use by any running program.
   */
  @DELETE
  @Path("/namespaces/{namespace-id}/profiles/{profile-name}")
  public void deleteProfile(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("profile-name") String profileName) throws Exception {
    ProfileId profileId = getValidatedProfile(namespaceId, profileName);
    accessEnforcer.enforce(profileId, authenticationContext.getPrincipal(), StandardPermission.DELETE);
    profileService.deleteProfile(profileId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/namespaces/{namespace-id}/profiles/{profile-name}/status")
  public void getProfileStatus(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("profile-name") String profileName) throws Exception {
    ProfileId profileId = getValidatedProfile(namespaceId, profileName);
    accessEnforcer.enforce(profileId, authenticationContext.getPrincipal(), StandardPermission.GET);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(profileService.getProfile(profileId).getStatus()));
  }

  /**
   * Disable the profile, so that no new program runs can use it,
   * and no new schedules/programs can be assigned to it.
   */
  @POST
  @Path("/namespaces/{namespace-id}/profiles/{profile-name}/disable")
  public void disableProfile(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("profile-name") String profileName) throws Exception {
    ProfileId profileId = getValidatedProfile(namespaceId, profileName);
    accessEnforcer.enforce(profileId,
                           authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    profileService.disableProfile(profileId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Enable the profile, so that programs/schedules can be assigned to it.
   */
  @POST
  @Path("/namespaces/{namespace-id}/profiles/{profile-name}/enable")
  public void enableProfile(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("profile-name") String profileName) throws Exception {
    ProfileId profileId = getValidatedProfile(namespaceId, profileName);
    accessEnforcer.enforce(profileId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    profileService.enableProfile(profileId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private NamespaceId getValidatedNamespace(String namespaceStr) throws BadRequestException, MethodNotAllowedException {
    try {
      NamespaceId namespaceId = new NamespaceId(namespaceStr);
      if (namespaceId.equals(NamespaceId.SYSTEM)) {
        throw new MethodNotAllowedException(
          "Cannot perform profile methods on the system namespace. "
            + "Please use the top level profile endpoints to manage system profiles.");
      }
      return namespaceId;
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
    }
  }

  private ProfileId getValidatedProfile(String namespaceStr, String profileName)
    throws BadRequestException, MethodNotAllowedException {
    return getValidatedProfile(getValidatedNamespace(namespaceStr), profileName);
  }

  private ProfileId getValidatedProfile(NamespaceId namespaceId, String profileName) throws BadRequestException {
    try {
      return namespaceId.profile(profileName);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
    }
  }

  private void writeProfile(ProfileId profileId, FullHttpRequest request)
    throws BadRequestException, IOException, MethodNotAllowedException {
    ProfileCreateRequest profileCreateRequest;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
      profileCreateRequest = GSON.fromJson(reader, ProfileCreateRequest.class);
      validateProvisionerProperties(profileCreateRequest);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Unable to parse request body. Please make sure it is valid JSON", e);
    }
    Profile profile =
      new Profile(profileId.getProfile(), profileCreateRequest.getLabel(), profileCreateRequest.getDescription(),
                  profileId.getScope(), profileCreateRequest.getProvisioner());
    profileService.saveProfile(profileId, profile);
  }

  private void validateProvisionerProperties(ProfileCreateRequest request) throws BadRequestException {
    try {
      request.validate();
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage());
    }
    ProvisionerInfo provisionerInfo = request.getProvisioner();
    Map<String, String> properties = new HashMap<>();
    Collection<ProvisionerPropertyValue> provisionerProperties = provisionerInfo.getProperties();
    if (provisionerProperties != null) {
      for (ProvisionerPropertyValue value : provisionerProperties) {
        if (value == null) {
          continue;
        }
        properties.put(value.getName(), value.getValue());
      }
    }
    try {
      provisioningService.validateProperties(provisionerInfo.getName(), properties);
    } catch (NotFoundException e) {
      throw new BadRequestException(String.format("The specified provisioner %s does not exist, " +
                                                    "thus cannot be associated with a profile",
                                                  provisionerInfo.getName()), e);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
    }
  }

  private void checkPutProfilePermissions(ProfileId profileId) {
    boolean profileExists;
    try {
      profileService.getProfile(profileId);
      profileExists = true;
    } catch (NotFoundException e) {
      profileExists = false;
    }
    if (profileExists) {
      accessEnforcer.enforce(profileId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    } else {
      accessEnforcer.enforce(profileId, authenticationContext.getPrincipal(), StandardPermission.CREATE);
    }
  }
}
