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
 * Provides an CCP (ClientCommandProcessor) address to packets. Address can be null.
 *
 * @author westy
 */
public abstract class BasicPacketWithReturnAddress extends BasicPacketWithClientAddress {

  /**
   * ccpAddress
   */
  private final static String RETURN_ADDRESS = "returnAddress";

  /**
   * returnPort
   */
  private final static String RETURN_PORT = "returnPort";

  /**
   * An invalid port.
   */
  protected final static int INVALID_PORT = -1;
  //
  /**
   * This is used by the Nameservers so they know which CCP to send the packet back to.
   * Replaces lnsId.
   */
  private InetSocketAddress returnAddress = null;

  /**
   * Creates a BasicPacketWithReturnAddress from a JSONObject.
   * 
   * @param json
   * @throws JSONException 
   */
  public BasicPacketWithReturnAddress(JSONObject json) throws JSONException {
    super(json);
    String address = json.optString(RETURN_ADDRESS, null);
    int port = json.optInt(RETURN_PORT, INVALID_PORT);
    this.returnAddress = address != null && port != INVALID_PORT
            ? new InetSocketAddress(address, port) : null;
  }
  
  /**
   * Creates a BasicPacketWithCCPAddress with a null return address.
   */
  public BasicPacketWithReturnAddress() {
    this.returnAddress = null;
  }
  
  /**
   * Creates a BasicPacketWithReturnAddress using address as the return address.
   *
   * @param address
   */
  public BasicPacketWithReturnAddress(InetSocketAddress address) {
    this.returnAddress = address;
  }
  

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    super.addToJSONObject(json);
    if (returnAddress != null) {
      json.put(RETURN_ADDRESS, returnAddress.getHostString());
      json.put(RETURN_PORT, returnAddress.getPort());
    }
  }

  /**
   * Get the address to which LNS to send the packet back to.
   *
   * @return an address
   */
  public InetSocketAddress getReturnAddress() {
    return returnAddress;
  }

}
