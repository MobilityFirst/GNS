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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.packet;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides the basics for Packets including a type field.
 * 
 * @author westy
 */
public abstract class BasicPacketWithSignatureInfo extends BasicPacketWithClientAddress {

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

  /**
   *
   * @param json
   * @throws JSONException
   */
  public BasicPacketWithSignatureInfo(JSONObject json) throws JSONException {
    super(json);
    this.accessor = json.optString(ACCESSOR, null);
    this.signature = json.optString(SIGNATURE, null);
    this.message = json.optString(MESSAGE, null);
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    super.addToJSONObject(json);
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
