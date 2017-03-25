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

import edu.umass.cs.nio.interfaces.Stringifiable;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides packet with an NameServerID.
 * NameServerID can't be null but can be -1 meaning an invalid name server.
 *
 * @author westy
 * @param <NodeIDType>
 */
@Deprecated
public abstract class BasicPacketWithNs<NodeIDType> extends BasicPacketWithClientAddress {

  /** ns_ID */
  private final static String NAMESERVER_ID = "ns_ID";

  /**
   * ID of name server receiving the message.
   * Often if this is null if the packet hasn't made it to the NS yet.
   */
  private NodeIDType nameServerID;

  /**
   * Creates an instance of BasicPacketWithNSAndCCP from a JSONObject.
   * 
   * @param json
   * @param unstringer
   * @throws JSONException 
   */
  public BasicPacketWithNs(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json);
    this.nameServerID = json.has(NAMESERVER_ID) ? unstringer.valueOf(json.getString(NAMESERVER_ID)) : null;
  }
  
  /**
   * Creates an instance of BasicPacketWithNSAndLnsAddress.
   *
   * @param nameServerID
   */
  public BasicPacketWithNs(NodeIDType nameServerID) {
    super();
    this.nameServerID = nameServerID;
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    super.addToJSONObject(json);
    if (nameServerID != null) {
      json.put(NAMESERVER_ID, nameServerID);
    }
  }

  /**
   * Get the id of the name server (usually the one that is handling this packet).
   *
   * @return a node id
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
}
