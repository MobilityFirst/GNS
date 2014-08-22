/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.packet;

import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides an LNS address to packets. Address can be null.
 *
 * @author westy
 */
public abstract class BasicPacketWithLnsAddress extends BasicPacket implements PacketInterface, ExtensiblePacketInterface {

  public final static String LNS_ADDRESS = "lnsAddress";
  public final static String LNS_PORT = "lnsPort";

  public final static int INVALID_PORT = -1;
  //
  /**
   * This is used by the Nameservers so they know which LNS to send the packet back to.
   * Replaces lnsId.
   */
  private InetSocketAddress lnsAddress = null;

  /**
   * Creates a BasicPacketWithLnsAddress.
   *
   * @param address
   */
  public BasicPacketWithLnsAddress(InetSocketAddress address) {
    this.lnsAddress = address;
  }

  /**
   * Creates a BasicPacketWithLnsAddress.
   *
   * @param address
   * @param port
   */
  public BasicPacketWithLnsAddress(String address, Integer port) {
    this(address != null && port != INVALID_PORT ? new InetSocketAddress(address, port) : null);
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    if (lnsAddress != null) {
      json.put(LNS_ADDRESS, lnsAddress.getHostString());
      json.put(LNS_PORT, lnsAddress.getPort());
    }
  }

  /**
   * Get the address to which LNS to send the packet back to.
   *
   * @return
   */
  public InetSocketAddress getLnsAddress() {
    return lnsAddress;
  }

  /**
   * Set the address to which LNS to send the packet back to.
   *
   * @param lnsAddress
   */
  public void setLnsAddress(InetSocketAddress lnsAddress) {
    this.lnsAddress = lnsAddress;
  }

}
