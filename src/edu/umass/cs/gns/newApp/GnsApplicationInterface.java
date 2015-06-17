/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp;

import edu.umass.cs.gns.newApp.clientCommandProcessor.ClientCommandProcessor;
import edu.umass.cs.gns.newApp.recordmap.BasicRecordMap;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.nio.InterfaceMessenger;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableNodeConfig;
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

  InterfaceMessenger<NodeIDType,JSONObject> getNioServer();
  
  PingManager getPingManager();
  
  ClientCommandProcessor getClientCommandProcessor();
  
}
