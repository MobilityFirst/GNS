/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

import edu.umass.cs.gnscommon.ResponseCode;

final class AclCheckResult {

  private final String publicKey;
  private final boolean aclCheckPassed;
  private final ResponseCode responseCode;

  public AclCheckResult(String publicKey, boolean aclCheckPassed, ResponseCode responseCode) {
    this.publicKey = publicKey;
    this.aclCheckPassed = aclCheckPassed;
    this.responseCode = responseCode;
  }

  public String getPublicKey() {
    return publicKey;
  }

  public boolean isAclCheckPassed() {
    return aclCheckPassed;
  }

  public ResponseCode getResponseCode() {
    return responseCode;
  }

}
