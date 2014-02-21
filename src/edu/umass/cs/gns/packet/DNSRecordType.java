/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.packet;

/**
 * This class describes the record types for DNS Packets.
 * @author Westy
 *
 */
public enum DNSRecordType {

  QUERY(0),
  RESPONSE(1);
  // stash the codes in a lookup table
  private static DNSRecordType[] typeCodes;

  static {
    typeCodes = new DNSRecordType[DNSRecordType.values().length];
    for (DNSRecordType code : DNSRecordType.values()) {
      typeCodes[code.getCodeValue()] = code;
    }
  }

  static DNSRecordType getRecordType(int codeValue) {
    return typeCodes[codeValue];
  }
  int codeValue;

  private DNSRecordType(int codeValue) {
    this.codeValue = codeValue;
  }

  public int getCodeValue() {
    return codeValue;
  }
}
