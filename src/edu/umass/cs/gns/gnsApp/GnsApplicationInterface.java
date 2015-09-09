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
  
  NodeIDType getNodeID();

  BasicRecordMap getDB();

  InterfaceReconfigurableNodeConfig<NodeIDType> getGNSNodeConfig();
  
  void sendToClient(InetSocketAddress isa, JSONObject msg) throws IOException;
  
  void sendToID(NodeIDType id, JSONObject msg) throws IOException;
  
  PingManager<NodeIDType> getPingManager();
  
  ClientCommandProcessor getClientCommandProcessor();
  
}
