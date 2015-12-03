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
 * Provides an CCP (ClientCommandProcessor) address to packets. Address can be null.
 *
 * @author westy
 */
public abstract class BasicPacketWithCCPAddress extends BasicPacketWithClientAddress {

  /**
   * ccpAddress
   */
  private final static String CCP_ADDRESS = "ccpAddress";

  /**
   * ccpPort
   */
  private final static String CCP_PORT = "ccpPort";

  /**
   * An invalid port.
   */
  protected final static int INVALID_PORT = -1;
  //
  /**
   * This is used by the Nameservers so they know which CCP to send the packet back to.
   * Replaces lnsId.
   */
  private InetSocketAddress ccpAddress = null;

  /**
   * Creates a BasicPacketWithCCPAddress from a JSONObject.
   * 
   * @param json
   * @throws JSONException 
   */
  public BasicPacketWithCCPAddress(JSONObject json) throws JSONException {
    super(json);
    String address = json.optString(CCP_ADDRESS, null);
    int port = json.optInt(CCP_PORT, INVALID_PORT);
    this.ccpAddress = address != null && port != INVALID_PORT
            ? new InetSocketAddress(address, port) : null;
  }
  
  /**
   * Creates a BasicPacketWithCCPAddress with a null CPP address.
   */
  public BasicPacketWithCCPAddress() {
    this.ccpAddress = null;
  }
  
  /**
   * Creates a BasicPacketWithCCPAddress using address as the CPP address.
   *
   * @param address
   */
  public BasicPacketWithCCPAddress(InetSocketAddress address) {
    this.ccpAddress = address;
  }
  

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    super.addToJSONObject(json);
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
  public InetSocketAddress getCppAddress() {
    return ccpAddress;
  }

  /**
   * Set the address to which LNS to send the packet back to.
   *
   * @param lnsAddress
   */
  public void setCppAddress(InetSocketAddress lnsAddress) {
    this.ccpAddress = lnsAddress;
  }

}
