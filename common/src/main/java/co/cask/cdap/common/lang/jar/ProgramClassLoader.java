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
import java.net.MalformedURLException;
import java.net.URLClassLoader;

/**
 * ClassLoader that implements bundle jar feature, in which the application jar contains
 * its dependency jars inside the "/lib" folder (by default) within the application jar.
 * <p/>
 * Useful for
 * 1) using third party jars that overwrite each other's files
 * (e.g. Datanucleus jars each have plugin.xml at same location
 * relative to the jar root, so if you package your application
 * as an uber-jar, your application jar will only contain one
 * of the plugin.xml at best unless you do some manual configuration.
 * <p/>
 * Not (yet) useful for
 * 1) avoiding classpath conflicts with Reactor's dependency jars
 * (e.g. you want to use Guava 16.0.1 but Reactor uses 13.0.1)
 */
public class ProgramClassLoader extends URLClassLoader {

  /**
   * Convenience class to construct a classloader for a program from an unpacked jar directory.
   * Adds <unpackedJarDir>/{.,*.jar,lib/*.jar} to the {@link URLClassLoader}.
   *
   * @param unpackedJarDir Directory of the unpacked jar to be used in the classpath.
   * @param parentDelegate Parent classloader.
   */
  public ProgramClassLoader(File unpackedJarDir, ClassLoader parentDelegate) throws MalformedURLException {
    super(ClassPathUrlsUtil.getClassPathUrls(unpackedJarDir), parentDelegate);
  }
}
