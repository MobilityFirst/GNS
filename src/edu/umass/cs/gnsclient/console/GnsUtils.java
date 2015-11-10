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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.console;

import java.util.regex.Pattern;

/**
 * @author westy
 */
public class GnsUtils
{

  private static Pattern hex = Pattern.compile("^[0-9A-Fa-f]+$");

  /**
   * Returns true if the given String is a potentially valid GUID (160 bits
   * expressed as a 40 digit hex number). Note that this does not check if the
   * GUID is valid in the GNS, just that this is a properly formed string.
   * 
   * @param guid the string to evaluate
   * @return true if it is a properly formatted GUID string
   */
  public static boolean isValidGuidString(String guid)
  {
    return (guid != null) && (guid.length() == 40)
        && hex.matcher(guid).matches();
  }
}
