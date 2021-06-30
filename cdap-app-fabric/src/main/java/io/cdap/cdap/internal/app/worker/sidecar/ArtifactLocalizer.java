/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.FileUtils;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequestConfig;
import org.jboss.resteasy.util.HttpHeaderNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * ArtifactLocalizer is responsible for fetching, caching and unpacking artifacts requested by the worker pod. The HTTP
 * endpoints are defined in {@link ArtifactLocalizerHttpHandlerInternal}. This class will run in the sidecar container
 * that is defined by {@link ArtifactLocalizerTwillRunnable}.
 *
 * Artifacts will be cached using the following file structure:
 * /DATA_DIRECTORY/artifacts/<namespace>/<artifact-name>/<artifact-version>/<last-modified-timestamp>.jar
 *
 * Artifacts will be unpacked using the following file structure:
 * /DATA_DIRECTORY/unpacked/<namespace>/<artifact-name>/<artifact-version>/<last-modified-timestamp>/...
 *
 * The procedure for fetching an artifact is:
 *
 * 1. Check if there is a locally cached version of the artifact, if so fetch the lastModified timestamp from
 * the filename.
 * 2. Send a request to the {@link io.cdap.cdap.gateway.handlers.ArtifactHttpHandlerInternal#getArtifactBytes} endpoint
 * in appfabric and provide the lastModified timestamp (if available)
 * 3. If a lastModified timestamp was not specified, or there is a newer version of the artifact: appfabric will stream
 * the bytes for the newest version of the jar and pass the new lastModified timestamp in the response headers
 *
 * OR
 *
 * If the provided lastModified timestamp matches the newest version of the artifact: appfabric will return
 * NOT_MODIFIED
 *
 * 4. Return the local path to the newest version of the artifact jar.
 *
 * NOTE: There is no need to invalidate the cache at any point since we will always need to call appfabric to confirm
 * that the cached version is the newest version available.
 */
public class ArtifactLocalizer {

  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizer.class);
  private static final String LAST_MODIFIED_HEADER = HttpHeaderNames.LAST_MODIFIED.toLowerCase();

  private final RemoteClient remoteClient;
  private final RetryStrategy retryStrategy;
  private final String dataDir;

  @Inject
  public ArtifactLocalizer(CConfiguration cConf, RemoteClientFactory remoteClientFactory) {
    this.remoteClient = remoteClientFactory.createRemoteClient(Constants.Service.APP_FABRIC_HTTP,
                                                               HttpRequestConfig.DEFAULT,
                                                               Constants.Gateway.INTERNAL_API_VERSION_3);
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, Constants.Service.TASK_WORKER + ".");
    this.dataDir = cConf.get(Constants.CFG_LOCAL_DATA_DIR);
  }

  /**
   * Gets the location on the local filesystem for the given artifact. This method handles fetching the artifact as well
   * as caching it.
   *
   * @param artifactId The ArtifactId of the artifact to fetch
   * @return The Local Location for this artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception while fetching or caching the artifact
   * @throws Exception if there was an unexpected error
   */
  public File getArtifact(ArtifactId artifactId) throws Exception {
    LOG.debug("Fetching artifact info for {}", artifactId);
    return Retries.callWithRetries(() -> fetchArtifact(artifactId), retryStrategy);
  }

  /**
   * Gets the location on the local filesystem for the directory that contains the unpacked artifact. This method
   * handles fetching, caching and unpacking the artifact.
   *
   * @param artifactId The ArtifactId of the artifact to fetch and unpack
   * @return The Local Location of the directory that contains the unpacked artifact files
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception while fetching, caching or unpacking the artifact
   * @throws Exception if there was an unexpected error
   */
  public File getAndUnpackArtifact(ArtifactId artifactId) throws Exception {
    LOG.debug("Unpacking artifact {}", artifactId);
    File jarLocation = getArtifact(artifactId);
    File unpackDir = getUnpackLocalPath(artifactId, Long.parseLong(jarLocation.getName().split("\\.")[0]));
    LOG.debug("Got unpack directory as {}", unpackDir);
    if (!unpackDir.exists()) {
      LOG.debug("Unpack directory doesn't exist, unpacking into {}", unpackDir);

      // Ensure the path leading to the unpack directory is created so we can create a temp directory
      File unpackDirParentFile = unpackDir.getParentFile();
      if (!unpackDirParentFile.exists() && !unpackDirParentFile.mkdirs()) {
        throw new IOException(String.format("Failed to create one or more directories along the path %s",
                                            unpackDirParentFile.getPath()));
      }
      File tempUnpackDir = DirUtils.createTempDir(unpackDirParentFile);
      try {
        BundleJarUtil.prepareClassLoaderFolder(jarLocation, tempUnpackDir);
        Files.move(tempUnpackDir.toPath(), unpackDir.toPath(), StandardCopyOption.ATOMIC_MOVE,
                   StandardCopyOption.REPLACE_EXISTING);
      } finally {
        // This directory should only exist if something went wrong with the move command above
        if (tempUnpackDir.exists()) {
          DirUtils.deleteDirectoryContents(tempUnpackDir);
        }
      }
    }
    return unpackDir;
  }

  /**
   * fetchArtifact attempts to connect to app fabric to download the given artifact. This method will throw {@link
   * RetryableException} in certain circumstances so using this with the
   *
   * @param artifactId the id of the artifact to fetch
   * @return a File pointing to the locally cached jar of the newest version
   * @throws IOException If an unexpected error occurs while writing the artifact to the filesystem
   * @throws ArtifactNotFoundException If the given artifact does not exist
   */
  private File fetchArtifact(ArtifactId artifactId) throws IOException, ArtifactNotFoundException {
    Long lastModifiedTimestamp = getCurrentLastModifiedTimestamp(artifactId);
    String namespaceId = artifactId.getNamespace();
    ArtifactScope scope = ArtifactScope.USER;
    // Cant use 'system' as the namespace in the request because that generates an error, the namespace doesnt matter
    // as long as it exists. Using default because it will always be there
    if (ArtifactScope.SYSTEM.toString().equalsIgnoreCase(namespaceId)) {
      namespaceId = NamespaceId.DEFAULT.getEntityName();
      scope = ArtifactScope.SYSTEM;
    }

    String url = String.format("namespaces/%s/artifacts/%s/versions/%s/download?scope=%s",
                               namespaceId,
                               artifactId.getArtifact(),
                               artifactId.getVersion(),
                               scope);

    HttpURLConnection urlConn = remoteClient.openConnection(HttpMethod.GET, url);
    try {
      if (lastModifiedTimestamp != null) {
        LOG.debug("Found existing local version for {} with timestamp {}", artifactId, lastModifiedTimestamp);

        ZonedDateTime lastModifiedDate = ZonedDateTime
          .ofInstant(Instant.ofEpochMilli(lastModifiedTimestamp), ZoneId.of("GMT"));
        urlConn.setRequestProperty(HttpHeaderNames.IF_MODIFIED_SINCE, lastModifiedDate.format(
          DateTimeFormatter.RFC_1123_DATE_TIME));
      }

      // If we get this response that means we already have the most up to date artifact
      if (urlConn.getResponseCode() == HttpURLConnection.HTTP_NOT_MODIFIED) {
        LOG.debug("Call to app fabric returned NOT_MODIFIED for {} with lastModifiedTimestamp of {}", artifactId,
                  lastModifiedTimestamp);
        File artifactJarLocation = getArtifactJarLocation(artifactId, lastModifiedTimestamp);
        if (!artifactJarLocation.exists()) {
          throw new RetryableException(String.format("Locally cached artifact jar for %s is missing.",
                                                     artifactId));
        }
        return artifactJarLocation;
      }

      throwIfError(urlConn, artifactId);

      ZonedDateTime newModifiedDate = getLastModifiedHeader(urlConn);
      long newTimestamp = newModifiedDate.toInstant().toEpochMilli();
      File newLocation = getArtifactJarLocation(artifactId, newTimestamp);
      DirUtils.mkdirs(newLocation.getParentFile());

      // Download the artifact to a temporary file then atomically rename it to the final name to
      // avoid race conditions with multiple threads.
      Path tempFile = Files.createTempFile(newLocation.getParentFile().toPath(), String.valueOf(newTimestamp), ".jar");
      try (InputStream in = urlConn.getInputStream()) {
        Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
        Files.move(tempFile, newLocation.toPath(), StandardCopyOption.ATOMIC_MOVE,
                   StandardCopyOption.REPLACE_EXISTING);
      } finally {
        Files.deleteIfExists(tempFile);
      }

      return newLocation;
    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * This checks the local cache for this artifact and retrieves the timestamp for the newest cache entry, if this
   * artifact is not cached it returns null
   */
  @Nullable
  private Long getCurrentLastModifiedTimestamp(ArtifactId artifactId) {
    File artifactDir = getArtifactDirLocation(artifactId);

    // Check if we have cached jars in the artifact directory, if so return the latest modified timestamp.
    return DirUtils.listFiles(artifactDir, File::isFile).stream()
      .map(FileUtils::getNameWithoutExtension)
      .map(Long::valueOf)
      .max(Long::compare)
      .orElse(null);
  }

  /**
   * Helper function for verifying, extracting and converting the Last-Modified header from the URL connection.
   */
  private ZonedDateTime getLastModifiedHeader(HttpURLConnection urlConn) {
    Map<String, List<String>> headers = urlConn.getHeaderFields();
    List<String> lastModifiedHeader = headers.entrySet().stream()
      .filter(headerEntry -> LAST_MODIFIED_HEADER.equalsIgnoreCase(headerEntry.getKey()))
      .map(Map.Entry::getValue)
      .findFirst()
      .orElse(null);

    if (lastModifiedHeader == null || lastModifiedHeader.size() != 1) {
      // This should never happen since this endpoint should always set the header.
      // If it does happen we should retry.
      throw new RetryableException(String.format("The response from %s did not contain the %s header.",
                                                 urlConn.getURL(), LAST_MODIFIED_HEADER));
    }

    return ZonedDateTime
      .parse(headers.get(LAST_MODIFIED_HEADER).get(0), DateTimeFormatter.RFC_1123_DATE_TIME);
  }

  /**
   * Helper method for catching and throwing any errors that might have occurred when attempting to connect.
   */
  private void throwIfError(HttpURLConnection urlConn, ArtifactId artifactId)
    throws IOException, ArtifactNotFoundException {
    int responseCode = urlConn.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_OK) {
      return;
    }

    String errMsg = CharStreams.toString(new InputStreamReader(urlConn.getErrorStream(), StandardCharsets.UTF_8));
    switch (responseCode) {
      case HttpURLConnection.HTTP_NOT_FOUND:
        throw new ArtifactNotFoundException(artifactId);
      case HttpURLConnection.HTTP_UNAVAILABLE:
        throw new ServiceUnavailableException(Constants.Service.APP_FABRIC_HTTP, errMsg);
    }
    throw new IOException(
      String.format("Failed to fetch artifact %s from app-fabric due to %s", artifactId, errMsg));
  }

  private Path getLocalPath(String dirName, ArtifactId artifactId) {
    return Paths.get(dataDir, dirName, artifactId.getNamespace(), artifactId.getArtifact(),
                     artifactId.getVersion());
  }

  /**
   * Returns a {@link File} representing the cache directory jars for the given artifact. The file path is:
   * /DATA_DIRECTORY/artifacts/<namespace>/<artifact-name>/<artifact-version>/
   */
  private File getArtifactDirLocation(ArtifactId artifactId) {
    return getLocalPath("artifacts", artifactId).toFile();
  }

  /**
   * Returns a {@link File} representing the cached jar for the given artifact and timestamp. The file path is:
   * /DATA_DIRECTORY/artifacts/<namespace>/<artifact-name>/<artifact-version>/<last-modified-timestamp>.jar
   */
  private File getArtifactJarLocation(ArtifactId artifactId, long lastModifiedTimestamp) {
    return getLocalPath("artifacts", artifactId).resolve(String.format("%d.jar", lastModifiedTimestamp)).toFile();
  }

  /**
   * Returns a {@link File} representing the directory containing the unpacked contents of the jar for the given
   * artifact and timestamp. The file path is:
   * /DATA_DIRECTORY/unpacked/<namespace>/<artifact-name>/<artifact-version>/<last-modified-timestamp>
   */
  private File getUnpackLocalPath(ArtifactId artifactId, long lastModifiedTimestamp) {
    return getLocalPath("unpacked", artifactId).resolve(String.valueOf(lastModifiedTimestamp)).toFile();
  }
}
