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

import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides a packet with signature info and an CCP address field.
 *
 * @author westy
 */
public abstract class BasicPacketWithSignatureInfoAndCCPAddress extends BasicPacketWithCCPAddress implements PacketInterface, ExtensiblePacketInterface {

  /** accessor */
  public final static String ACCESSOR = "si_accessor";

  /** signature */
  public final static String SIGNATURE = "si_signature";

  /** message */
  public final static String MESSAGE = "si_message";
  //
  private String accessor;
  private String signature;
  private String message;

  /**
   * Construct this guy with the address, but no signature info.
   *
   * @param ccpAddress
   */
  public BasicPacketWithSignatureInfoAndCCPAddress(InetSocketAddress ccpAddress) {
    this(ccpAddress, null, null, null);
  }

  /**
   * Construct this with the address and all the signature info.
   *
   * @param ccpAddress
   * @param accessor
   * @param signature
   * @param message
   */
  public BasicPacketWithSignatureInfoAndCCPAddress(InetSocketAddress ccpAddress, String accessor, String signature, String message) {
    super(ccpAddress);
    this.accessor = accessor;
    this.signature = signature;
    this.message = message;
  }

  /**
   * Construct this with all the address and signature info.
   *
   * @param address
   * @param port
   * @param accessor
   * @param signature
   * @param message
   */
  public BasicPacketWithSignatureInfoAndCCPAddress(String address, Integer port, String accessor, String signature, String message) {
    this(address != null && port != INVALID_PORT ? new InetSocketAddress(address, port) : null,
            accessor, signature, message);
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    addToJSONObject(json, true);
  }

  /**
   * Add this packets fields to a Json object. 
   * 
   * @param json
   * @param includeSignatureSection
   * @throws JSONException
   */
  public void addToJSONObject(JSONObject json, boolean includeSignatureSection) throws JSONException {
    super.addToJSONObject(json);
    if (includeSignatureSection) {
      if (accessor != null) {
        json.put(ACCESSOR, accessor);
      }
      if (signature != null) {
        json.put(SIGNATURE, signature);
      }
      if (message != null) {
        json.put(MESSAGE, message);
      }
    }
  }

  /**
   * Return the accessor.
   * 
   * @return a string
   */
  public String getAccessor() {
    return accessor;
  }

  /**
   * Return the signature.
   * 
   * @return a string
   */
  public String getSignature() {
    return signature;
  }

  /**
   * Return the message.
   * 
   * @return a string
   */
  public String getMessage() {
    return message;
  }
}
