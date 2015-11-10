/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

/**
 * Support for fields that can't be accessed by the client.
 * 
 * @author westy
 */
public class InternalField {
  private static final String INTERNAL_PREFIX = "_GNS_";

  /**
   * Creates a GNS field that is hidden from the user.
   *
   * @param string
   * @return a string
   */
  public static String makeInternalFieldString(String string) {
    return INTERNAL_PREFIX + string;
  }

  /**
   * Returns true if field is a GNS field that is hidden from the user.
   *
   * @param key
   * @return true if field is a GNS field that is hidden from the user
   */
  public static boolean isInternalField(String key) {
    return key.startsWith(INTERNAL_PREFIX);
  }

  /**
   * Returns the length of the prefix used with internal fields.
   * 
   * @return an int
   */
  public static int getPrefixLength() {
    return INTERNAL_PREFIX.length();
  }
}
