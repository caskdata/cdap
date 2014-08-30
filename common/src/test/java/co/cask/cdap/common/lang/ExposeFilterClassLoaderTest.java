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

package co.cask.cdap.common.lang;

import co.cask.cdap.api.annotation.ExposeClass;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.lang.jar.ExposeFilterClassLoader;
import co.cask.cdap.common.lang.jar.JarFinder;
import co.cask.cdap.common.utils.DirUtils;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * Testing the exposed and unexposed annotations for Dataset Filtered Class loading
 */
public class ExposeFilterClassLoaderTest {

  // todo - the test needs to be fixed, maybe we can take bundle jar approach and have a jar file in resources
  @Test
  public void testExposedDataset() throws ClassNotFoundException, IOException {
    String jarPath = JarFinder.getJar(ExposedDataset.class);
    File dsFile = Files.createTempDir();
    try {
      Set<String> annotations = Sets.newHashSet();
      annotations.add(ExposeClass.class.getName());
      Predicate<String> annotationPredicate = Predicates.in(annotations);
      LocationFactory lf = new LocalLocationFactory();
      BundleJarUtil.unpackProgramJar(lf.create(jarPath), dsFile);
      ClassLoader dsClassLoader = new ExposeFilterClassLoader(dsFile, null,
                                                              annotationPredicate);
      dsClassLoader.loadClass(ExposedDataset.class.getName());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally {
      DirUtils.deleteDirectoryContents(dsFile);
    }
  }

  @Test(expected = ClassNotFoundException.class)
  public void testUnExposedDataset() throws ClassNotFoundException, IOException {

    Set<String> annotations = Sets.newHashSet();
    annotations.add(ExposeClass.class.getName());
    Predicate<String> annotationPredicate = Predicates.in(annotations);

//    String jarPath = JarFinder.getJar(UnExposedDataset.class);
    String jarPath = "/Users/shankar/project/cdap/examples/Purchase/target/Purchase-2.5.0-SNAPSHOT.jar";
    LocationFactory lf = new LocalLocationFactory();
    File dsFile = Files.createTempDir();
    try {
      BundleJarUtil.unpackProgramJar(lf.create(jarPath), dsFile);
      ClassLoader dsClassLoader = new ExposeFilterClassLoader(dsFile, null, annotationPredicate);
      dsClassLoader.loadClass("co.cask.cdap.examples.purchase.PurchaseHistory");
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally {
      DirUtils.deleteDirectoryContents(dsFile);
    }

  }

  @ExposeClass
  class ExposedDataset {
    public void test() {

    }
  }

  class UnExposedDataset {
    public void test() {

    }
  }
}
