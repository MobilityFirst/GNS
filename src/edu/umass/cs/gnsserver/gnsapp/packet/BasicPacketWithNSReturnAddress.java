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
 * Provides an NS return address to packets. Address can be null.
 *
 * @author westy
 */
public abstract class BasicPacketWithNSReturnAddress extends BasicPacketWithClientAddress {

  /**
   * address
   */
  private final static String NS_RETURN_ADDRESS = "ns_r_address";

  /**
   * port
   */
  private final static String NS_RETURN_PORT = "ns_r_port";

  /**
   * An invalid port.
   */
  protected final static int INVALID_PORT = -1;

  /**
   * This is used by the Select so they know which NS to send the packet back to.
   */
  private InetSocketAddress nameServerReturnAddress = null;

  /**
   * Creates a BasicPacketWithReturnAddress from a JSONObject.
   *
   * @param json
   * @throws JSONException
   */
  public BasicPacketWithNSReturnAddress(JSONObject json) throws JSONException {
    super(json);
    String address = json.optString(NS_RETURN_ADDRESS, null);
    int port = json.optInt(NS_RETURN_PORT, INVALID_PORT);
    this.nameServerReturnAddress = address != null && port != INVALID_PORT
            ? new InetSocketAddress(address, port) : null;
  }

  /**
   * Creates a BasicPacketWithCCPAddress with a null return address.
   */
  public BasicPacketWithNSReturnAddress() {
    super();
    this.nameServerReturnAddress = null;
  }

  /**
   * Creates a BasicPacketWithReturnAddress using address as the return address.
   *
   * @param address
   */
  public BasicPacketWithNSReturnAddress(InetSocketAddress address) {
    super();
    this.nameServerReturnAddress = address;
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    super.addToJSONObject(json);
    if (nameServerReturnAddress != null) {
      json.put(NS_RETURN_ADDRESS, nameServerReturnAddress.getHostString());
      json.put(NS_RETURN_PORT, nameServerReturnAddress.getPort());
    }
  }

  /**
   * Get the address to which LNS to send the packet back to.
   *
   * @return an address
   */
  public InetSocketAddress getNSReturnAddress() {
    return nameServerReturnAddress;
  }
  
  /**
   * Sets the id of the name server (usually the one that is handling this packet).
   *
   * @param address
   */
  public void setNSReturnAddress(InetSocketAddress address) {
    this.nameServerReturnAddress = address;
  }

}
