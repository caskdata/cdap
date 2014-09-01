/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.common.lang.jar;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

/**
 * Utility class that can be used to get array of {@link java.net.URL} given the unpacked jar location,
 * used to create {@link java.net.URLClassLoader}
 */
public class ClassPathUrlsUtil {

  public static URL[] getClassPathUrls(File unpackedJarDir) throws MalformedURLException {
    List<URL> classPathUrls = new LinkedList<URL>();
    classPathUrls.add(unpackedJarDir.toURI().toURL());
    classPathUrls.addAll(getJarURLs(unpackedJarDir));
    classPathUrls.addAll(getJarURLs(new File(unpackedJarDir, "lib")));
    return classPathUrls.toArray(new URL[classPathUrls.size()]);
  }

  private static List<URL> getJarURLs(File dir) throws MalformedURLException {
    File[] files = dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".jar");
      }
    });
    List<URL> urls = new LinkedList<URL>();

    if (files != null) {
      for (File file : files) {
        urls.add(file.toURI().toURL());
      }
    }
    return urls;
  }
}
