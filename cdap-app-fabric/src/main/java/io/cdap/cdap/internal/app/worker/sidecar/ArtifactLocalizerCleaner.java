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

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotNull;

/**
 * Artifact cleaner that will deleted out-of-date cache entries that were localized by {@link ArtifactLocalizer}
 */
public class ArtifactLocalizerCleaner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizerCleaner.class);

  private final Path cacheDir;
  private final int cacheCleanupInterval;

  public ArtifactLocalizerCleaner(CConfiguration cConf) {
    cacheDir = Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR));
    cacheCleanupInterval = cConf.getInt(Constants.ArtifactLocalizer.CACHE_CLEANUP_INTERVAL_MIN);
  }

  @Override
  public void run() {
    try {
      cleanupArtifactCache(cacheDir.resolve("artifacts").toFile());
    } catch (Exception e) {
      LOG.warn("ArtifactLocalizerService failed to clean up cache. Will retry again in {} minutes: {}",
               cacheCleanupInterval, e);
    }
  }

  /**
   * Recursively looks for out of date artifact jar files in cacheDir. Once a jar file is found, we will first attempt
   * to delete the unpacked directory for this jar (if it exists) and then delete the jar if and only if that was
   * successful.
   *
   * @param cacheDir The directory that contains the cached jar files
   */
  private void cleanupArtifactCache(@NotNull File cacheDir) throws IOException {
    List<Long> timestamps = new ArrayList<>();

    // Scan all files in the current directory, if its a directory recurse, otherwise add the timestamp value to a list
    List<File> files = DirUtils.listFiles(cacheDir);
    for (File file : files) {
      if (file.isDirectory()) {
        cleanupArtifactCache(file);
        continue;
      }

      // We're only interested in jar files
      if (!file.getName().endsWith(".jar")) {
        continue;
      }
      try {
        timestamps.add(Long.parseLong(FileUtils.getNameWithoutExtension(file)));
      } catch (NumberFormatException e) {
        // If we encounter a file that doesn't have a timestamp as the filename
        LOG.warn("Encountered unexpected file during artifact cache cleanup: {}. This file will be deleted.",
                 file.getPath());
        Files.deleteIfExists(file.toPath());
      }
    }

    // If there are less than two timestamp files then we don't need to clean anything
    if (timestamps.size() < 2) {
      return;
    }

    // Only keep the file that has the highest timestamp (the newest file)
    String maxTimestamp = timestamps.stream().max(Long::compare).get().toString();
    List<File> filesToDelete = DirUtils.listFiles(cacheDir, (dir, name) -> !name.startsWith(maxTimestamp));
    for (File file : filesToDelete) {
      // Only delete the jar file if we successfully deleted the unpacked directory for this artifact
      if (deleteUnpackedCacheDir(file)) {
        LOG.debug("Deleting JAR file {}", file.getPath());
        Files.deleteIfExists(file.toPath());
      }
    }
  }

  /**
   * Deletes the unpacked directory that corresponds to the given jar file
   *
   * @param jarPath the artifact jar file which will be used to construct the unpacked directory path
   * @return true if the delete was successful or the unpacked directory does not exist, false otherwise
   */
  private boolean deleteUnpackedCacheDir(@NotNull File jarPath) {
    String unpackedPath = jarPath.getParentFile().getPath().replaceFirst("artifacts", "unpacked");
    String unpackedDirName = FileUtils.getNameWithoutExtension(jarPath);

    // If this directory doesn't exist then this artifact was never unpacked and we can return true
    File dirPath = Paths.get(unpackedPath, unpackedDirName).toFile();
    if (!dirPath.exists()) {
      LOG.debug("Unpacked directory does not exist, no need to delete: {}", dirPath.getPath());
      return true;
    }

    LOG.debug("Deleting unpacked directory for jar {}", jarPath.getPath());
    try {
      DirUtils.deleteDirectoryContents(dirPath);
      LOG.debug("Successfully deleted unpacked directory {}", dirPath.getPath());
      return true;
    } catch (IOException e) {
      LOG.warn("Encountered error when attempting to delete unpacked dir {}: {}", dirPath.getPath(), e);
    }
    return false;
  }
}
