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

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import java.io.IOException;
import org.json.JSONObject;

/**
 * This pulls out some methods from GnsReconfigurableInterface that were needed for
 * transition to new app framework.
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
   * Returns the record map.
   *
   * @return the record map
   */
  BasicRecordMap getDB();

  /**
   * Returns the node config.
   *
   * @return the node config
   */
  ReconfigurableNodeConfig<NodeIDType> getGNSNodeConfig();

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
   * @param id
   * @param msg
   * @throws IOException
   */
  void sendToID(NodeIDType id, JSONObject msg) throws IOException;

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
