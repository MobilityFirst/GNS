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

import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class represents the DNS packet header.
 */
public class Header {

  private static final String ID = "id";
  private static final String QRCODE = "qr";
  private static final String RESPONSECODE = "rcode";

  /**
   * Unique ID for each query. Used by the requester to match up replies to outstanding queries *
   */
  private int id;
  /**
   * Specifies whether this message is a query or a response
   */
  private DNSRecordType queryResponseCode;
  /**
   * Response code
   */
  private NSResponseCode responseCode;

  /**
   * Constructs a Header for the resource record packet with the specified fields.
   *
   * @param id Unique ID for each query
   * @param qr Specifies whether this message is a query (0), or a response (1)
   * @param rcode Response code
   *
   */
  public Header(int id, DNSRecordType qr, NSResponseCode rcode) {
    this.id = id;
    this.queryResponseCode = qr;
    this.responseCode = rcode;
  }

  /**
   * Constructs a Header for the resource record packet from a JSONObject that represents the packet header
   *
   * @param json JSONObject that represents the packet header
   * @throws org.json.JSONException
   *
   */
  public Header(JSONObject json) throws JSONException {
    this.id = json.getInt(ID);
    // stored as an int in the JSON to keep the byte counting folks happy
    this.queryResponseCode = DNSRecordType.getRecordType(json.getInt(QRCODE));
    // stored as an int in the JSON to keep the byte counting folks happy
    this.responseCode = NSResponseCode.getResponseCode(json.getInt(RESPONSECODE));
  }

  /**
   * Converts the packet header to a JSONObject that represents the header.
   *
   * @return Return a JSONObject that represents the packet header
   * @throws org.json.JSONException
   *
   */
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(ID, getId());
    // store it as an int in the JSON to keep the byte counting folks happy
    json.put(QRCODE, getQueryResponseCode().getCodeValue());
    // store it as an int in the JSON to keep the byte counting folks happy
    json.put(RESPONSECODE, getResponseCode().getCodeValue());
    return json;
  }

  /**
   * Returns a string representing the packet header.
   *
   * @return a string
   */
  @Override
  public String toString() {
    try {
      return this.toJSONObject().toString();
    } catch (JSONException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Return the id.
   * 
   * @return the id
   */
  public int getId() {
    return id;
  }

  /**
   * Set the id.
   * 
   * @param id the id to set
   */
  public void setId(int id) {
    this.id = id;
  }

  /**
   * Return the query reponse code.
   * 
   * @return the qr
   */
  public DNSRecordType getQueryResponseCode() {
    return queryResponseCode;
  }

  /**
   * Return the query/response code.
   * 
   * @param qr the qr to set
   */
  public void setQueryResponseCode(DNSRecordType qr) {
    this.queryResponseCode = qr;
  }

  /**
   * Return the name server response code.
   * @return the rcode
   */
  public NSResponseCode getResponseCode() {
    return responseCode;
  }

  /**
   * Set the name server response code.
   * 
   * @param rcode the rcode to set
   */
  public void setResponseCode(NSResponseCode rcode) {
    this.responseCode = rcode;
  }

  /**
   * Return true if the request is a query.
   *
   * @return true if the request is a query
   */
  public boolean isQuery() {
    return queryResponseCode == DNSRecordType.QUERY;
  }

  /**
   * Returns true if the packet is a response, false otherwise.
   *
   * @return true if the packet is a response
   */
  public boolean isResponse() {
    return queryResponseCode == DNSRecordType.RESPONSE;
  }

  /**
   * Returns true if the packet contains any kind of response error, false otherwise
   *
   * @return true if the packet contains any kind of response error
   */
  public boolean isAnyKindOfError() {
    return responseCode.isAnError();
  }

  /**
   * Return true if the result is Invalid Active NSError.
   *
   * @return true if the result is Invalid Active NSError
   */
  public boolean isInvalidActiveNSError() {
    return responseCode == NSResponseCode.ERROR_INVALID_ACTIVE_NAMESERVER;
  }
}
