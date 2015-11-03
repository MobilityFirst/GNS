/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp.packet;

import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides a packet with signature info and a NameServerID and LNS address field.
 *
 * @author westy
 * @param <NodeIDType>
 */
public abstract class BasicPacketWithSignatureInfoAndNSAndCCP<NodeIDType> extends BasicPacketWithNSAndCCP<NodeIDType> 
implements PacketInterface, ExtensiblePacketInterface {

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

  /**
   * Construct this with all the address and signature info.
   *
   * @param nameServerID
   * @param address
   * @param port
   * @param accessor
   * @param signature
   * @param message
   */
  public BasicPacketWithSignatureInfoAndNSAndCCP(NodeIDType nameServerID, String address, Integer port, String accessor, String signature, String message) {
    this(nameServerID, address != null && port != INVALID_PORT ? new InetSocketAddress(address, port) : null,
            accessor, signature, message);
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    addToJSONObject(json, true);
  }

  /**
   * Add this packets fields to a Json object.
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
