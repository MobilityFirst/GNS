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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.packet;

import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides packet with NameServer Address and return address. 
 *
 * @author westy
 */
public abstract class BasicPacketWithReturnAddressAndNsAddress extends BasicPacketWithReturnAddress {

  /**
   * address
   */
  private final static String NS_ADDRESS = "ns_address";

  /**
   * port
   */
  private final static String NS_PORT = "ns_port";

  /**
   * This is used by the Select so they know which NS to send the packet back to.
   */
  private InetSocketAddress nameServerAddress = null;

  /**
   * Creates a BasicPacketWithReturnAddress from a JSONObject.
   *
   * @param json
   * @throws JSONException
   */
  public BasicPacketWithReturnAddressAndNsAddress(JSONObject json) throws JSONException {
    super(json);
    String address = json.optString(NS_ADDRESS, null);
    int port = json.optInt(NS_PORT, INVALID_PORT);
    this.nameServerAddress = address != null && port != INVALID_PORT
            ? new InetSocketAddress(address, port) : null;
  }

  /**
   * Creates a BasicPacketWithCCPAddress with a null return address.
   */
  public BasicPacketWithReturnAddressAndNsAddress() {
    super();
    this.nameServerAddress = null;
  }

  /**
   * Creates a BasicPacketWithReturnAddress with an NS address and client return address.
   *
   * @param address
   * @param clientAddress
   */
  public BasicPacketWithReturnAddressAndNsAddress(InetSocketAddress address, 
          InetSocketAddress clientAddress) {
    super(clientAddress);
    this.nameServerAddress = address;
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    super.addToJSONObject(json);
    if (nameServerAddress != null) {
      json.put(NS_ADDRESS, nameServerAddress.getHostString());
      json.put(NS_PORT, nameServerAddress.getPort());
    }
  }

  /**
   * Get the address to which LNS to send the packet back to.
   *
   * @return an address
   */
  public InetSocketAddress getNSAddress() {
    return nameServerAddress;
  }
  
  /**
   * Sets the id of the name server (usually the one that is handling this packet).
   *
   * @param address
   */
  public void setNSAddress(InetSocketAddress address) {
    this.nameServerAddress = address;
  }

}
