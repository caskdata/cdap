/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.config;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProfileConflictException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.store.profile.ProfileDataset;
import co.cask.cdap.internal.profile.AdminEventPublisher;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.runtime.spi.profile.ProfileStatus;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * This class is to manage preference related functions. It will wrap the {@link PreferencesDataset} operation
 * in transaction in each method
 */
public class PreferencesService {

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final AdminEventPublisher adminEventPublisher;

  @Inject
  public PreferencesService(DatasetFramework datasetFramework, TransactionSystemClient txClient,
                            MessagingService messagingService,
                            CConfiguration cConf) {
    this.datasetFramework = datasetFramework;
    MultiThreadMessagingContext messagingContext = new MultiThreadMessagingContext(messagingService);
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
        txClient, NamespaceId.SYSTEM,
        Collections.emptyMap(), null, null, messagingContext)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.adminEventPublisher = new AdminEventPublisher(cConf, messagingContext);
  }

  private Map<String, String> getConfigProperties(EntityId entityId) {
    return Transactionals.execute(transactional, context -> {
      return PreferencesDataset.get(context, datasetFramework).getPreferences(entityId);
    });
  }

  private Map<String, String> getConfigResolvedProperties(EntityId entityId) {
    return Transactionals.execute(transactional, context -> {
      return PreferencesDataset.get(context, datasetFramework).getResolvedPreferences(entityId);
    });
  }

  /**
   * Validate the profile status is enabled and set the preferences in same transaction
   */
  private void setConfig(EntityId entityId,
                         Map<String, String> propertyMap) throws NotFoundException, ProfileConflictException {
    Transactionals.execute(transactional, context -> {
      ProfileDataset profileDataset = ProfileDataset.get(context, datasetFramework);
      PreferencesDataset preferencesDataset = PreferencesDataset.get(context, datasetFramework);

      boolean isInstanceLevel = entityId.getEntityType().equals(EntityType.INSTANCE);
      NamespaceId namespaceId = isInstanceLevel ?
        NamespaceId.SYSTEM : ((NamespacedEntityId) entityId).getNamespaceId();

      // validate the profile and publish the necessary metadata change if the profile exists in the property
      Optional<ProfileId> profile = SystemArguments.getProfileIdFromArgs(namespaceId, propertyMap);
      if (profile.isPresent()) {
        ProfileId profileId = profile.get();
        // if it is instance level set, the profile has to be in SYSTEM scope, so throw BadRequestException when
        // setting a USER scoped profile
        if (isInstanceLevel && !propertyMap.get(SystemArguments.PROFILE_NAME).startsWith(EntityScope.SYSTEM.name())) {
          throw new BadRequestException(String.format("Cannot set profile %s at the instance level. " +
                                                        "Only system profiles can be set at the instance level. " +
                                                        "The profile property must look like SYSTEM:[profile-name]",
                                                      propertyMap.get(SystemArguments.PROFILE_NAME)));
        }

        if (profileDataset.getProfile(profileId).getStatus() == ProfileStatus.DISABLED) {
          throw new ProfileConflictException(String.format("Profile %s in namespace %s is disabled. It cannot be " +
                                                             "assigned to any programs or schedules",
                                                           profileId.getProfile(), profileId.getNamespace()),
                                             profileId);
        }
      }

      // need to get old property and check if it contains profile information
      Map<String, String> oldProperties = preferencesDataset.getPreferences(entityId);
      // get the old profile information from the previous properties
      Optional<ProfileId> oldProfile = SystemArguments.getProfileIdFromArgs(namespaceId, oldProperties);
      preferencesDataset.setPreferences(entityId, propertyMap);

      // After everything is set, publish the update message if profile is present
      profile.ifPresent(profileId -> adminEventPublisher.publishProfileAssignment(entityId, profileId));

      // if new profiles do not have profile information but old profiles have, it is same as deletion of the profile
      if (!profile.isPresent() && oldProfile.isPresent()) {
        adminEventPublisher.publishProfileUnAssignment(entityId);
      }
    }, NotFoundException.class, ProfileConflictException.class);
  }

  private void deleteConfig(EntityId entityId) {
    Transactionals.execute(transactional, context -> {
      PreferencesDataset dataset = PreferencesDataset.get(context, datasetFramework);
      Map<String, String> oldProp = dataset.getPreferences(entityId);
      dataset.deleteProperties(entityId);
      if (oldProp.containsKey(SystemArguments.PROFILE_NAME)) {
        adminEventPublisher.publishProfileUnAssignment(entityId);
      }
    });
  }

  /**
   * Get instance level preferences
   */
  public Map<String, String> getProperties() {
    return getConfigProperties(new InstanceId(""));
  }

  /**
   * Get namespace level preferences
   */
  public Map<String, String> getProperties(NamespaceId namespaceId) {
    return getConfigProperties(namespaceId);
  }

  /**
   * Get app level preferences
   */
  public Map<String, String> getProperties(ApplicationId applicationId) {
    return getConfigProperties(applicationId);
  }

  /**
   * Get program level preferences
   */
  public Map<String, String> getProperties(ProgramId programId) {
    return getConfigProperties(programId);
  }

  /**
   * Get instance level resolved preferences
   */
  public Map<String, String> getResolvedProperties() {
    return getConfigResolvedProperties(new InstanceId(""));
  }

  /**
   * Get namespace level resolved preferences
   */
  public Map<String, String> getResolvedProperties(NamespaceId namespaceId) {
    return getConfigResolvedProperties(namespaceId);
  }

  /**
   * Get app level resolved preferences
   */
  public Map<String, String> getResolvedProperties(ApplicationId appId) {
    return getConfigResolvedProperties(appId);
  }

  /**
   * Get program level resolved preferences
   */
  public Map<String, String> getResolvedProperties(ProgramId programId) {
   return getConfigResolvedProperties(programId);
  }

  /**
   * Set instance level preferences
   */
  public void setProperties(Map<String, String> propMap) throws NotFoundException, ProfileConflictException {
    setConfig(new InstanceId(""), propMap);
  }

  /**
   * Set namespace level preferences
   */
  public void setProperties(NamespaceId namespaceId,
                            Map<String, String> propMap) throws NotFoundException, ProfileConflictException {
    setConfig(namespaceId, propMap);
  }

  /**
   * Set app level preferences
   */
  public void setProperties(ApplicationId appId, Map<String, String> propMap)
    throws NotFoundException, ProfileConflictException {
    setConfig(appId, propMap);
  }

  /**
   * Set program level preferences
   */
  public void setProperties(ProgramId programId,
                            Map<String, String> propMap) throws NotFoundException, ProfileConflictException {
    setConfig(programId, propMap);
  }

  /**
   * Delete instance level preferences
   */
  public void deleteProperties() {
    deleteConfig(new InstanceId(""));
  }

  /**
   * Delete namespace level preferences
   */
  public void deleteProperties(NamespaceId namespaceId) {
    deleteConfig(namespaceId);
  }

  /**
   * Delete app level preferences
   */
  public void deleteProperties(ApplicationId appId) {
    deleteConfig(appId);
  }

  /**
   * Delete program level preferences
   */
  public void deleteProperties(ProgramId programId) {
    deleteConfig(programId);
  }
}
