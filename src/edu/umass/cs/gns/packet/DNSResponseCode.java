package edu.umass.cs.gns.packet;

/*************************************************************
 * This class describes the error codes for the resource record packet.
 * @author Hardeep Uppal
 *
 ************************************************************/
public enum DNSResponseCode {

  NO_ERROR(0, false),
  ERROR(1, true),
  ERROR_INVALID_ACTIVE_NAMESERVER(2, true),
  SIGNATURE_ERROR(3, true),
  ACCESS_ERROR(4, true);
  //
  // stash the codes in a lookup table
  private static DNSResponseCode[] responseCodes;

  static {
    responseCodes = new DNSResponseCode[DNSResponseCode.values().length];
    for (DNSResponseCode code : DNSResponseCode.values()) {
      responseCodes[code.getCodeValue()] = code;
    }
  }
  
  static DNSResponseCode getResponseCode(int codeValue) {
    return responseCodes[codeValue];
  }
  //
  int codeValue;
  boolean isAnError;

  private DNSResponseCode(int codeValue, boolean isAnError) {
    this.codeValue = codeValue;
    this.isAnError = isAnError;
  }
  
  public int getCodeValue() {
    return codeValue;
  }

  public boolean isAnError() {
    return isAnError;
  }
  
  
}
