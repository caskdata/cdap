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

package io.cdap.cdap.securestore.gcp.cloudkms;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.api.services.cloudkms.v1.CloudKMSScopes;
import com.google.api.services.cloudkms.v1.model.CryptoKey;
import com.google.api.services.cloudkms.v1.model.DecryptRequest;
import com.google.api.services.cloudkms.v1.model.DecryptResponse;
import com.google.api.services.cloudkms.v1.model.EncryptRequest;
import com.google.api.services.cloudkms.v1.model.EncryptResponse;
import com.google.api.services.cloudkms.v1.model.KeyRing;
import com.google.common.io.CharStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Wrapper on {@link CloudKMS} client.
 */
class CloudKMSClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CloudKMSClient.class);
  // When created in the global location, Cloud KMS resources are available from zones spread around the world.
  private static final String LOCATION_ID = "global";
  // All the crypto keys are created under keyring named cdap
  private static final String ENCRYPT_DECRYPT = "ENCRYPT_DECRYPT";
  private static final String CLOUD_KMS = "cloudkms";
  private static final String PROJECT_ID = "project.id";
  private static final String SERVICE_ACCOUNT_FILE = "service.account.file";
  private static final String METADATA_SERVER_API = "metadata.server.api";
  private static final String DEFAULT_METADATA_SERVER_API = "http://metadata.google.internal/computeMetadata" +
    "/v1/project/project-id";
  private static final String KEYRING_ID = "keyring.id";
  private static final String DEFAULT_KEYRING_ID = "cdap";


  private final CloudKMS cloudKMS;
  private final String projectId;
  private final String keyringId;
  // In-memory cache to hold created crypto keys, this is to avoid checking if a given crypto key exists.
  private final Set<String> knownCryptoKeys;

  /**
   * Constructs Cloud KMS client.
   *
   * @throws IOException if cloud kms client can not be created
   */
  CloudKMSClient(Map<String, String> properties) throws IOException {
    String metadataServerApi = properties.getOrDefault(METADATA_SERVER_API, DEFAULT_METADATA_SERVER_API);
    this.projectId = properties.containsKey(PROJECT_ID) ? properties.get(PROJECT_ID) :
      getSystemProjectId(metadataServerApi);
    String serviceAccountFile = properties.getOrDefault(SERVICE_ACCOUNT_FILE, null);
    this.cloudKMS = createCloudKMS(serviceAccountFile);
    this.keyringId = properties.getOrDefault(KEYRING_ID, DEFAULT_KEYRING_ID);
    this.knownCryptoKeys = new HashSet<>();
  }

  /**
   * Get project id from the metadata server. Makes a request to the metadata server that lives on the VM,
   * as described at https://cloud.google.com/compute/docs/storing-retrieving-metadata.
   */
  private String getSystemProjectId(String metadataServerApi) throws IOException {
    URL url = new URL(metadataServerApi);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    try {
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestProperty("Metadata-Flavor", "Google");
      connection.connect();
      try (Reader reader = new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8)) {
        return CharStreams.toString(reader);
      }
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Creates an authorized CloudKMS client service.
   *
   * @return an authorized CloudKMS client
   * @throws IOException if credentials can not be created in current environment
   */
  private CloudKMS createCloudKMS(String serviceAccountFile) throws IOException {
    HttpTransport transport = new NetHttpTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    GoogleCredential credential;

    if (serviceAccountFile == null) {
      credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
    } else {
      File credentialsPath = new File(serviceAccountFile);
      try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
        credential = GoogleCredential.fromStream(serviceAccountStream, transport, jsonFactory);
      }
    }

    if (credential.createScopedRequired()) {
      credential = credential.createScoped(CloudKMSScopes.all());
    }

    return new CloudKMS.Builder(transport, jsonFactory, credential)
      .setApplicationName(CLOUD_KMS)
      .build();
  }

  /**
   * Creates a new key ring with the given id.
   *
   * @throws IOException if there's an error while creating the key ring
   */
  void createKeyRingIfNotExists() throws IOException {
    String parent = String.format("projects/%s/locations/%s", projectId, LOCATION_ID);
    LOG.debug("Creating key ring with id {}.", keyringId);

    // Check if key exists before attempting to create one. This provides for the case where a keyring is provided, but
    // the authenticating account does not have permission to create keyrings on the target kms project.
    KeyRing keyRing = null;
    try {
      keyRing = cloudKMS.projects().locations().keyRings()
              .get(keyringId)
              .execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getDetails() == null || e.getDetails().getCode() != 404) {
        // If the call fails, then we should proceed to attempt creation
        throw new IOException(String.format("Exception occurred while checking for key ring %s", keyringId), e);
      }
    }

    if (Objects.isNull(keyRing)) {
      try {
        cloudKMS.projects().locations().keyRings()
                .create(parent, new KeyRing())
                .setKeyRingId(keyringId)
                .execute();
      } catch (GoogleJsonResponseException e) {
        // if key ring already exists, then do not throw any exception.
        if (e.getDetails() != null && e.getDetails().getCode() == 409) {
          LOG.trace(String.format("Key ring %s already exists", keyringId));
          return;
        }
        throw new IOException(String.format("Exception occurred while creating key ring %s", keyringId), e);
      }
    }
  }

  /**
   * Creates a new crypto key on google cloud kms with the given id.
   *
   * @param cryptoKeyId crypto key id
   * @throws IOException if there's an error creating crypto key
   */
  void createCryptoKeyIfNotExists(String cryptoKeyId) throws IOException {
    // If crypto key is already created, do not attempt to create it again.
    if (knownCryptoKeys.contains(cryptoKeyId)) {
      return;
    }

    String parent = String.format("projects/%s/locations/%s/keyRings/%s", projectId, LOCATION_ID, keyringId);

    CryptoKey cryptoKey = new CryptoKey();
    // This will allow the API access to the key for symmetric encryption and decryption.
    cryptoKey.setPurpose(ENCRYPT_DECRYPT);

    try {
      cloudKMS.projects().locations().keyRings().cryptoKeys()
        .create(parent, cryptoKey)
        .setCryptoKeyId(cryptoKeyId)
        .execute();
    } catch (GoogleJsonResponseException e) {
      // Crypto key is shared for all the secrets in a namespace. If the crypto key already exists, then do not throw
      // any exception. This will happen if another key for the same namespace is being created.
      if (e.getDetails() != null && e.getDetails().getCode() == 409) {
        LOG.trace(String.format("Key %s already exists", cryptoKeyId));
        return;
      }

      throw new IOException("Error occurred while creating cryptographic key for namespace %s" , e);
    }

    // In-memory cache to keep list of crypto keys created so far.
    knownCryptoKeys.add(cryptoKeyId);
  }

  /**
   * Encrypts secret with provided crypto key.
   *
   * @param cryptoKeyId crypto key to encrypt secret
   * @param secret secret to be encrypted
   * @throws IOException there's an error in encrypting secret
   */
  byte[] encrypt(String cryptoKeyId, byte[] secret) throws IOException {
    String resourceName = String.format("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", projectId, LOCATION_ID,
                                        keyringId, cryptoKeyId);
    // secret must not be longer than 64KiB.
    EncryptRequest request = new EncryptRequest().encodePlaintext(secret);
    EncryptResponse response = cloudKMS.projects().locations().keyRings().cryptoKeys()
      .encrypt(resourceName, request)
      .execute();

    byte[] encryptedData = response.decodeCiphertext();
    if (encryptedData == null) {
      throw new IOException("Error while encrypting the secret. Encrypted data is null.");
    }

    return encryptedData;
  }

  /**
   * Decrypts the provided encrypted secret with the specified crypto key.
   *
   * @param cryptoKeyId crypto key to decrypt secret
   * @param encryptedSecret encrypted secret
   * @return decrypted secret
   * @throws IOException there's an error in decrypting secret
   */
  byte[] decrypt(String cryptoKeyId, byte[] encryptedSecret) throws IOException {
    String resourceName = String.format("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
                                        projectId, LOCATION_ID, keyringId, cryptoKeyId);

    DecryptRequest request = new DecryptRequest().encodeCiphertext(encryptedSecret);
    DecryptResponse response = cloudKMS.projects().locations().keyRings().cryptoKeys()
      .decrypt(resourceName, request)
      .execute();

    byte[] decrypted = response.decodePlaintext();
    if (decrypted == null) {
      throw new IOException("Error while decrypting the secret. Decrypted data is null");
    }

    return decrypted;
  }

  @Override
  public void close() throws IOException {
    knownCryptoKeys.clear();
  }
}
