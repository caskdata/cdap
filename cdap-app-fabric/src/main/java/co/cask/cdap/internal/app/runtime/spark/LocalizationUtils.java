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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Utilities for file localization.
 */
public final class LocalizationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(LocalizationUtils.class);

  /**
   * Used in local mode to copy resources to a temporary local directory.
   * TODO: CDAP-4178 Also handle decompression of archives.
   *
   * @param name the name of the resource to copy
   * @param resource the {@link LocalizeResource} for the resource to localize
   * @param targetDir the destination directory to localize the resource in
   * @return {@link File} pointing to the localized resource
   */
  public static File localizeResource(String name, LocalizeResource resource, File targetDir) throws IOException {
    File tempFile = new File(targetDir, name);
    File localizedFile = new File(resource.getURI().getPath());
    LOG.debug("Copy file from {} to {}", resource.getURI(), tempFile);
    Files.copy(localizedFile, tempFile);
    return localizedFile;
  }

  private LocalizationUtils() {
  }
}
