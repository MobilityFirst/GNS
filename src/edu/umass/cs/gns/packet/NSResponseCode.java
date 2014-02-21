/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.clientprotocol.Defs;

/**
 * This class describes the error codes for Name Server packets that
 * get sent back to the LNS and the client.
 * 
 * To save space in packets, ahem, we also provide the ability to convert back and forth between
 * each code and an integer version of it.
 * 
 * @author Westy
 *
 */
public enum NSResponseCode {

  NO_ERROR(0, "", false),
  ERROR(1, Defs.GENERICEERROR, true),
  ERROR_INVALID_ACTIVE_NAMESERVER(2, Defs.GENERICEERROR, true),
  SIGNATURE_ERROR(3, Defs.BADSIGNATURE, true),
  ACCESS_ERROR(4, Defs.BADREADERGUID, true),
  BAD_GUID_ERROR(5, Defs.BADGUID, true),
  BAD_ACCESOR_ERROR(6, Defs.ACCESSDENIED, true);
  //
  // stash the codes in a lookup table
  private static NSResponseCode[] responseCodes;

  static {
    responseCodes = new NSResponseCode[NSResponseCode.values().length];
    for (NSResponseCode code : NSResponseCode.values()) {
      responseCodes[code.getCodeValue()] = code;
    }
  }

  static NSResponseCode getResponseCode(int codeValue) {
    return responseCodes[codeValue];
  }
  //
  private int codeValue;
  private String protocolCode;
  private boolean isAnError;

  private NSResponseCode(int codeValue, String protocolCode, boolean isAnError) {
    this.codeValue = codeValue;
    this.protocolCode = protocolCode;
    this.isAnError = isAnError;
  }

  public int getCodeValue() {
    return codeValue;
  }

  public String getProtocolCode() {
    return protocolCode;
  }

  public boolean isAnError() {
    return isAnError;
  }
}
