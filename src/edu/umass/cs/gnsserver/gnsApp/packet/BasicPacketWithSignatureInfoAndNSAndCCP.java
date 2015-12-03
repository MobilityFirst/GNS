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

import edu.umass.cs.nio.interfaces.Stringifiable;
import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides a packet with signature info and a NameServerID and LNS address field.
 *
 * @author westy
 * @param <NodeIDType>
 */
public abstract class BasicPacketWithSignatureInfoAndNSAndCCP<NodeIDType> extends BasicPacketWithNSAndCCP<NodeIDType> {

  /** accessor */
  private final static String ACCESSOR = "si_accessor";

  /** signature */
  private final static String SIGNATURE = "si_signature";

  /** message */
  private final static String MESSAGE = "si_message";
  //
  private final String accessor;
  private final String signature;
  private final String message;

  public BasicPacketWithSignatureInfoAndNSAndCCP(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json, unstringer);
    this.accessor = json.optString(ACCESSOR, null);
    this.signature = json.optString(SIGNATURE, null);
    this.message = json.optString(MESSAGE, null);
  }
  /**
   * Construct this guy with the address, but no signature info.
   *
   * @param nameServerID
   * @param lnsAddress
   */
  public BasicPacketWithSignatureInfoAndNSAndCCP(NodeIDType nameServerID, InetSocketAddress lnsAddress) {
    this(nameServerID, lnsAddress, null, null, null);
  }

  /**
   * Construct this with the address and all the signature info.
   *
   * @param nameServerID
   * @param lnsAddress
   * @param accessor
   * @param signature
   * @param message
   */
  public BasicPacketWithSignatureInfoAndNSAndCCP(NodeIDType nameServerID, InetSocketAddress lnsAddress, String accessor, String signature, String message) {
    super(nameServerID, lnsAddress);
    this.accessor = accessor;
    this.signature = signature;
    this.message = message;
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    super.addToJSONObject(json);
    addToJSONObject(json, true);
  }

  /**
   * Add this packets fields to a JSON object.
   * Optionally include the signature section.
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
