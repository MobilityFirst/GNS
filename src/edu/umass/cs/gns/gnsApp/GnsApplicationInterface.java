/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.ClientCommandProcessor;
import edu.umass.cs.gns.gnsApp.recordmap.BasicRecordMap;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.reconfiguration.interfaces.InterfaceReconfigurableNodeConfig;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.json.JSONObject;

/**
 * This pulls out some methods from GnsReconfigurableInterface that were needed for
 * transition to new app framework.
 * 
 * @author westy
 * @param <NodeIDType>
 */
public interface GnsApplicationInterface<NodeIDType> {
  
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
  InterfaceReconfigurableNodeConfig<NodeIDType> getGNSNodeConfig();
  
  /**
   * Sends a JSON packet to a client.
   * 
   * @param isa
   * @param msg
   * @throws IOException
   */
  void sendToClient(InetSocketAddress isa, JSONObject msg) throws IOException;
  
  /**
   * Sends a JSON packet to a node.
   * 
   * @param id
   * @param msg
   * @throws IOException
   */
  void sendToID(NodeIDType id, JSONObject msg) throws IOException;
  
  /**
   * Returns the ping manager.
   * @see PingManager
   * 
   * @return the ping manager
   */
  PingManager<NodeIDType> getPingManager();
  
  /**
   * Returns the client command processor.
   * @see ClientCommandProcessor
   * 
   * @return the client command processor
   */
  ClientCommandProcessor getClientCommandProcessor();
  
}
