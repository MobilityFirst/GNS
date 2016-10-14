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
package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnscommon.utils.Base64;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 *
 * Also in the PHP client:
 * hash('sha256', $password . "42shabiz" . $username)
 * @author westy
 */
public class Password {

  private static final String SALT = "42shabiz";

  /**
   *
   * @param password
   * @param alias
   * @return the password encrypted and encoded using base64
   * @throws NoSuchAlgorithmException
   */
  public static String encryptAndEncodePassword(String password, String alias) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update((password + SALT + alias).getBytes());
    return Base64.encodeToString(md.digest(), false);
  }

}
