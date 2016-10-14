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

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author westy
 */
public class SHA1HashFunction extends BasicHashFunction {

  private MessageDigest hashfunction;

  private SHA1HashFunction() {
    try {
      hashfunction = MessageDigest.getInstance("SHA1");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   *
   * @param key
   * @return an array of bytes
   */
  @Override
  public synchronized byte[] hash(String key) {
    try {
      hashfunction.update(key.getBytes("UTF-8"));
      return hashfunction.digest();
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }

  }

  /**
   *
   * @param bytes
   * @return an array of bytes
   */
  public synchronized byte[] hash(byte[] bytes) {
    hashfunction.update(bytes);
    return hashfunction.digest();
  }

  /**
   *
   * @return the hash function instance
   */
  public static SHA1HashFunction getInstance() {
    return SHA1HashFunctionHolder.INSTANCE;
  }

  private static class SHA1HashFunctionHolder {

    private static final SHA1HashFunction INSTANCE = new SHA1HashFunction();
  }
}
