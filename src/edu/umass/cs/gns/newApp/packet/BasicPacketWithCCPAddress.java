/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp.packet;

import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides an LNS address to packets. Address can be null.
 *
 * @author westy
 */
public abstract class BasicPacketWithCCPAddress extends BasicPacket implements PacketInterface, ExtensiblePacketInterface {

  public final static String CCP_ADDRESS = "ccpAddress";
  public final static String CCP_PORT = "ccpPort";

  public final static int INVALID_PORT = -1;
  //
  /**
   * This is used by the Nameservers so they know which CCP to send the packet back to.
   * Replaces lnsId.
   */
  private InetSocketAddress ccpAddress = null;

  /**
   * Creates a BasicPacketWithLnsAddress.
   *
   * @param address
   */
  public BasicPacketWithCCPAddress(InetSocketAddress address) {
    this.ccpAddress = address;
  }

  /**
   * Creates a BasicPacketWithLnsAddress.
   *
   * @param address
   * @param port
   */
  public BasicPacketWithCCPAddress(String address, Integer port) {
    this(address != null && port != INVALID_PORT ? new InetSocketAddress(address, port) : null);
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    if (ccpAddress != null) {
      json.put(CCP_ADDRESS, ccpAddress.getHostString());
      json.put(CCP_PORT, ccpAddress.getPort());
    }
  }

  /**
   * Get the address to which LNS to send the packet back to.
   *
   * @return
   */
  public InetSocketAddress getCCPAddress() {
    return ccpAddress;
  }

  /**
   * Set the address to which LNS to send the packet back to.
   *
   * @param lnsAddress
   */
  public void setCCPAddress(InetSocketAddress lnsAddress) {
    this.ccpAddress = lnsAddress;
  }

}
