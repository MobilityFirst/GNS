/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp.packet;

import edu.umass.cs.gns.gnsApp.NSResponseCode;
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
  private DNSRecordType qrCode;
  /**
   * Response code *
   */
  private NSResponseCode responseCode;

  /**
   * ***********************************************************
   * Constructs a Header for the resource record packet with the specified fields.
   *
   * @param id Unique ID for each query
   * @param qr Specifies whether this message is a query (0), or a response (1)
   * @param rcode Response code
	 ***********************************************************
   */
  public Header(int id, DNSRecordType qr, NSResponseCode rcode) {
    this.id = id;
    this.qrCode = qr;
    this.responseCode = rcode;
  }

  /**
   * ***********************************************************
   * Constructs a Header for the resource record packet from a JSONObject that represents the packet header
   *
   * @param json JSONObject that represents the packet header
   * @throws org.json.JSONException
	 ***********************************************************
   */
  public Header(JSONObject json) throws JSONException {
    this.id = json.getInt(ID);
    // stored as an int in the JSON to keep the byte counting folks happy
    this.qrCode = DNSRecordType.getRecordType(json.getInt(QRCODE));
    // stored as an int in the JSON to keep the byte counting folks happy
    this.responseCode = NSResponseCode.getResponseCode(json.getInt(RESPONSECODE));
  }

  /**
   * ***********************************************************
   * Converts the packet header to a JSONObject that represents the header.
   *
   * @return Return a JSONObject that represents the packet header
   * @throws org.json.JSONException
	 ***********************************************************
   */
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(ID, getId());
    // store it as an int in the JSON to keep the byte counting folks happy
    json.put(QRCODE, getQRCode().getCodeValue());
    // store it as an int in the JSON to keep the byte counting folks happy
    json.put(RESPONSECODE, getResponseCode().getCodeValue());
    return json;
  }

  /**
   * ***********************************************************
   * Returns a string representing the packet header.
	 ***********************************************************
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
   * @return the id
   */
  public int getId() {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(int id) {
    this.id = id;
  }

  /**
   * @return the qr
   */
  public DNSRecordType getQRCode() {
    return qrCode;
  }

  /**
   * @param qr the qr to set
   */
  public void setQRCode(DNSRecordType qr) {
    this.qrCode = qr;
  }

  /**
   * @return the rcode
   */
  public NSResponseCode getResponseCode() {
    return responseCode;
  }

  /**
   * @param rcode the rcode to set
   */
  public void setResponseCode(NSResponseCode rcode) {
    this.responseCode = rcode;
  }

   public boolean isQuery() {
    return qrCode == DNSRecordType.QUERY;
  }

  /**
   * Returns true if the packet is a response, false otherwise
   */
  public boolean isResponse() {
    return qrCode == DNSRecordType.RESPONSE;
  }

  /**
   * Returns true if the packet contains any kind of response error, false otherwise
   *
   */
  public boolean isAnyKindOfError() {
    return responseCode.isAnError();
  }

  /**
   *
   * @return
   */
  public boolean isInvalidActiveNSError() {
    return responseCode == NSResponseCode.ERROR_INVALID_ACTIVE_NAMESERVER;
  }
}
