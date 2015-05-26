/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp.packet;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides the basics for Packets including a type field.
 * 
 * @author westy
 */
public abstract class BasicPacketWithSignatureInfo extends BasicPacket implements PacketInterface, ExtensiblePacketInterface {

  public final static String ACCESSOR = "si_accessor";
  public final static String SIGNATURE = "si_signature";
  public final static String MESSAGE = "si_message";
  //
  private String accessor;
  private String signature;
  private String message;

  /**
   * Construct this guy with no signature info.
   */
  public BasicPacketWithSignatureInfo() {
    this(null, null, null);
  }
  
  
  /**
   * Construct this with all the signature info.
   * 
   * @param accessor
   * @param signature
   * @param message 
   */
  public BasicPacketWithSignatureInfo(String accessor, String signature, String message) {
    this.accessor = accessor;
    this.signature = signature;
    this.message = message;
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
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

  public String getAccessor() {
    return accessor;
  }

  public String getSignature() {
    return signature;
  }

  public String getMessage() {
    return message;
  }
}
