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
public abstract class BasicPacketWithCPPAddress extends BasicPacket implements PacketInterface, ExtensiblePacketInterface {

  public final static String CPP_ADDRESS = "cppAddress";
  public final static String CPP_PORT = "cppPort";

  public final static int INVALID_PORT = -1;
  //
  /**
   * This is used by the Nameservers so they know which CPP to send the packet back to.
   * Replaces lnsId.
   */
  private InetSocketAddress cppAddress = null;

  /**
   * Creates a BasicPacketWithLnsAddress.
   *
   * @param address
   */
  public BasicPacketWithCPPAddress(InetSocketAddress address) {
    this.cppAddress = address;
  }

  /**
   * Creates a BasicPacketWithLnsAddress.
   *
   * @param address
   * @param port
   */
  public BasicPacketWithCPPAddress(String address, Integer port) {
    this(address != null && port != INVALID_PORT ? new InetSocketAddress(address, port) : null);
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    if (cppAddress != null) {
      json.put(CPP_ADDRESS, cppAddress.getHostString());
      json.put(CPP_PORT, cppAddress.getPort());
    }
  }

  /**
   * Get the address to which LNS to send the packet back to.
   *
   * @return
   */
  public InetSocketAddress getCPPAddress() {
    return cppAddress;
  }

  /**
   * Set the address to which LNS to send the packet back to.
   *
   * @param lnsAddress
   */
  public void setCPPAddress(InetSocketAddress lnsAddress) {
    this.cppAddress = lnsAddress;
  }

}
