/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.localNameServer;

import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DeleteServiceName;
import java.io.IOException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 * @param <NodeIDType>
 */
public interface EnhancedClientRequestHandlerInterface<NodeIDType> extends ClientRequestHandlerInterface<NodeIDType> {
  
  public NodeIDType getRandomReplica();

  public NodeIDType getRandomRCReplica();

  public NodeIDType getFirstReplica();

  public NodeIDType getFirstRCReplica();

  public CreateServiceName makeCreateNameRequest(String name, String state);

  public DeleteServiceName makeDeleteNameRequest(String name);
  
  public void sendRequest(BasicReconfigurationPacket req) throws JSONException, IOException;
  
  public boolean handleEvent(JSONObject json) throws JSONException;

}
