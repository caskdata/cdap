/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.common.utils;

import java.io.File;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Set;

/**
 * Utility class for File-related operations.
 */
public class FileUtils {

  /**
   * FileAttribute representing owner-only read/write (600). Note that this should not be used to create directories.
   */
  public static final FileAttribute<Set<PosixFilePermission>> OWNER_ONLY_RW =
    PosixFilePermissions.asFileAttribute(EnumSet.of(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ));

  private FileUtils() { }

  /**
   * Converts a permission, interpreted as a 3-digit octal, into a umask that yields this permission.
   *
   * @param permissions the permissions
   * @return the corrsponding umask
   */
  public static int permissionsToUmask(int permissions) {
    // the umask contains exactly the bits that the permissions don't have
    return permissions ^ 0777; // note this is octal
  }

  /**
   * Converts a permission string, given either as a 9-character String such as "rwxr-x---" or as
   * an octal-base number such as 750 (interpreted as a 3-digit octal) into a umask that yields
   * this permission as a 3-digit octal string.
   *
   * @param permissions the permissions
   * @return the corrsponding umask
   *
   * @throws NumberFormatException if the length of the permissions is 3 but it is not a valid octal number
   * @throws IllegalArgumentException if the permissions do not have a length of 3 or 9, or if it is a 9-character
   *         string that contains illegal characters (it must be of the form "rwxrwxrwx", where any character
   *         may be replaced with a "-")
   */
  public static String permissionsToUmask(String permissions) {
    return String.format("%03o", permissionsToUmask(parsePermissions(permissions)));
  }

  /**
   * Parses a permission string, given either as a 9-character String such as "rwxr-x---" or as
   * an octal-base number such as 750 (interpreted as a 3-digit octal).
   *
   * @param permissions the permissions
   * @return the integer representation of the permissions
   *
   * @throws NumberFormatException if the length of the permissions is 3 but it is not a valid octal number
   * @throws IllegalArgumentException if the permissions do not have a length of 3 or 9, or if it is a 9-character
   *         string that contains illegal characters (it must be of the form "rwxrwxrwx", where any character
   *         may be replaced with a "-")
   */
  public static int parsePermissions(String permissions) {
    if (permissions.length() == 3) {
      return Integer.parseInt(permissions, 8);
    } else if (permissions.length() == 9) {
      int perms = parsePermissionGroup(permissions, 0);
      perms = 8 * perms + parsePermissionGroup(permissions, 3);
      return 8 * perms + parsePermissionGroup(permissions, 6);
    } else {
      throw new IllegalArgumentException("Not a valid permissions string: " + permissions);
    }
  }

  /**
   * Returns the name of a given file without the extension, if one is present
   * @param file the file
   * @return the name of the file without the extension
   */
  public static String getNameWithoutExtension(File file) {
    String fileName = file.getName();
    if (file.isDirectory() || !fileName.contains(".")) {
      return fileName;
    }
    return fileName.substring(0, fileName.lastIndexOf("."));
  }

  private static int parsePermissionGroup(String permissions, int start) {
    return parseBit(permissions, start, 'r', 4)
      + parseBit(permissions, start + 1, 'w', 2)
      + parseBit(permissions, start + 2, 'x', 1);
  }

  private static int parseBit(String permissions, int index, char expected, int value) {
    if (permissions.charAt(index) == expected) {
      return value;
    } else if (permissions.charAt(index) == '-') {
      return 0;
    }
    throw new IllegalArgumentException("Not a valid permissions string: " + permissions);
  }

}
