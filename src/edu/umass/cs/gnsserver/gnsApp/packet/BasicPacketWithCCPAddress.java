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

import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides an LNS address to packets. Address can be null.
 *
 * @author westy
 */
public abstract class BasicPacketWithCCPAddress extends BasicPacket implements PacketInterface, ExtensiblePacketInterface {

  /** ccpAddress */
  public final static String CCP_ADDRESS = "ccpAddress";

  /** ccpPort */
  public final static String CCP_PORT = "ccpPort";

  /**
   * An invalid port.
   */
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
   * @return an address
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
