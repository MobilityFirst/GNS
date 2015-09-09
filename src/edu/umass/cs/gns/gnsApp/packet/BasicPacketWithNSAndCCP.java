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
 * Provides packet with an and a NameServerID and LNS address. Address can be null. 
 * NameServerID can't be null but can be INVALID_PORT.
 *
 * @author westy
 * @param <NodeIDType>
 */
public abstract class BasicPacketWithNSAndCCP<NodeIDType> extends BasicPacket implements PacketInterface, ExtensiblePacketInterface {

  public final static String NAMESERVER_ID = "ns_ID";
  public final static String CCP_ADDRESS = "ccpAddress";
  public final static String CCP_PORT = "ccpPort";

  public final static int INVALID_PORT = -1;

  /**
   * ID of name server receiving the message.
   * Often if this is null if the packet hasn't made it to the NS yet.
   */
  private NodeIDType nameServerID;
  //
  /**
   * This is used by the Nameservers so they know which LNS to send the packet back to.
   * Replaces lnsId.
   */
  private InetSocketAddress ccpAddress = null;

  /**
   * Creates a BasicPacketWithNSAndLnsAddress.
   *
   * @param address
   */
  public BasicPacketWithNSAndCCP(NodeIDType nameServerID, InetSocketAddress address) {
    this.nameServerID = nameServerID;
    this.ccpAddress = address;
  }

  /**
   * Creates a BasicPacketWithNSAndLnsAddress.
   *
   * @param address
   * @param port
   */
  public BasicPacketWithNSAndCCP(NodeIDType nameServerID, String address, Integer port) {
    this(nameServerID, address != null && port != INVALID_PORT ? new InetSocketAddress(address, port) : null);
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    if (nameServerID != null) {
      json.put(NAMESERVER_ID, nameServerID);
    }
    if (ccpAddress != null) {
      json.put(CCP_ADDRESS, ccpAddress.getHostString());
      json.put(CCP_PORT, ccpAddress.getPort());
    }
  }

  /**
   * Get the id of the name server (usually the one that is handling this packet).
   *
   * @return
   */
  public NodeIDType getNameServerID() {
    return nameServerID;
  }

  /**
   * Sets the id of the name server (usually the one that is handling this packet).
   *
   * @param nameServerID
   */
  public void setNameServerID(NodeIDType nameServerID) {
    this.nameServerID = nameServerID;
  }

  /**
   * Get the address to which LNS to send the packet back to.
   *
   * @return
   */
  public InetSocketAddress getCppAddress() {
    return ccpAddress;
  }

  /**
   * Set the address to which LNS to send the packet back to.
   *
   * @param ccpAddress
   */
  public void setCcpAddress(InetSocketAddress ccpAddress) {
    this.ccpAddress = ccpAddress;
  }

}
