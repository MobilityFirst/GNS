/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides packet with an and a NameServerID and LNS address. Address can be null. NameServerID can't be null
 * but can be GNSNodeConfig.INVALID_NAME_SERVER_ID.
 *
 * @author westy
 */
public abstract class BasicPacketWithNSAndLNS extends BasicPacket implements PacketInterface, ExtensiblePacketInterface {

  public final static String NAMESERVER_ID = "ns_ID";
  public final static String LNS_ADDRESS = "lnsAddress";
  public final static String LNS_PORT = "lnsPort";

  
  public final static int INVALID_PORT = -1;
  
  /**
   * ID of name server receiving the message.
   * Often if this is GNSNodeConfig.INVALID_NAME_SERVER_ID if the packet hasn't made it to the NS yet.
   */
  private NodeId<String> nameServerID;
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
  public BasicPacketWithNSAndLNS(NodeId<String> nameServerID, InetSocketAddress address) {
    this.nameServerID = nameServerID;
    this.lnsAddress = address;
  }

  /**
   * Creates a BasicPacketWithLnsAddress.
   *
   * @param address
   * @param port
   */
  public BasicPacketWithNSAndLNS(NodeId<String> nameServerID, String address, Integer port) {
    this(nameServerID, address != null && port != INVALID_PORT ? new InetSocketAddress(address, port) : null);
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    json.put(NAMESERVER_ID, nameServerID.toString());
    if (lnsAddress != null) {
      json.put(LNS_ADDRESS, lnsAddress.getHostString());
      json.put(LNS_PORT, lnsAddress.getPort());
    }
  }
  
  /**
   * Get the id of the name server (usually the one that is handling this packet).
   * 
   * @return 
   */
  public NodeId<String> getNameServerID() {
    return nameServerID;
  }
  
  /**
   * Sets the id of the name server (usually the one that is handling this packet).
   * @param nameServerID 
   */
  public void setNameServerID(NodeId<String> nameServerID) {
    this.nameServerID = nameServerID;
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
