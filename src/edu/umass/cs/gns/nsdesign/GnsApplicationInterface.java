/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.newApp.recordmap.BasicRecordMap;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;

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

  InterfaceJSONNIOTransport getNioServer();
  
  PingManager getPingManager();
  
}
