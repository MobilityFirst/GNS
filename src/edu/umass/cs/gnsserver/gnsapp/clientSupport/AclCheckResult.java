
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

import edu.umass.cs.gnscommon.ResponseCode;


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
