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
package edu.umass.cs.gnsserver.gnsapp;

import edu.umass.cs.contextservice.integration.ContextServiceGNSInterface;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;

import java.io.IOException;

import java.net.InetSocketAddress;
import org.json.JSONObject;

/**
 * This encapsulates the core functionality needed by the GNS Application.
 *
 * @author westy
 * @param <NodeIDType>
 */
public interface GNSApplicationInterface<NodeIDType> {

  /**
   * Returns the node id.
   *
   * @return the node id
   */
  NodeIDType getNodeID();
  
  /**
   * Returns the node address.
   * 
   * @return 
   */
  InetSocketAddress getNodeAddress();

  /**
   * Returns the record map.
   *
   * @return the record map
   */
  BasicRecordMap getDB();

  /**
   * Sends a JSON packet to a client.
   *
   * @param response
   * @param msg
   * @throws IOException
   */
  void sendToClient(Request response, JSONObject msg) throws IOException;
  
   /**
   * Sends a JSON packet to a node.
   *
   * @param address
   * @param msg
   * @throws IOException
   */
  void sendToAddress(InetSocketAddress address, JSONObject msg) throws IOException;

  /**
   * @param originalRequest
   * @param response
   * @param responseJSON
   * @throws IOException
   */
  public void sendToClient(CommandPacket originalRequest, Request response, JSONObject responseJSON)
		  throws IOException;
  
  /**
   * @return ContextServiceGNSInterface
   */
  public ContextServiceGNSInterface getContextServiceGNSClient();
  
  /**
   * Returns the request handler.
   *
   * @return the request handler
   */
  ClientRequestHandlerInterface getRequestHandler();

  /**
   * Returns the active code handler.
   *
   * @return the active code handler
   */
  ActiveCodeHandler getActiveCodeHandler();

}
