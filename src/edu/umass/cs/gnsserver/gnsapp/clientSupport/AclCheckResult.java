/*
 *
 *  Copyright (c) 2016 University of Massachusetts
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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

import edu.umass.cs.gnscommon.ResponseCode;

/**
 * Used in {@link NSAuthentication} to return multiple values from
 * ACL checks.
 * 
 * @author westy
 */
public final class AclCheckResult {

  private final String publicKey;
  private final ResponseCode responseCode;

  public AclCheckResult(String publicKey, ResponseCode responseCode) {
    this.publicKey = publicKey;
    this.responseCode = responseCode;
  }

  public String getPublicKey() {
    return publicKey;
  }

  public ResponseCode getResponseCode() {
    return responseCode;
  }

}
