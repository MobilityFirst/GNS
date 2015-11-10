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
package edu.umass.cs.gnsserver.gnsApp.packet;

/**
 * This class describes the record types for DNS Packets.
 * @author Westy
 *
 */
public enum DNSRecordType {

  /** query */
  QUERY(0),

  /** response */
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

  /**
   * Return the code value.
   * 
   * @return the code value
   */
  public int getCodeValue() {
    return codeValue;
  }
}
