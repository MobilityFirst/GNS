package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;

import edu.umass.cs.gns.nio.GenericMessagingTask;

/**
@author V. Arun
 */
public interface InterfaceMessenger<NodeIDType> {
	public void send(GenericMessagingTask<NodeIDType,InterfaceRequest> mtask) throws IOException, RequestParseException; 
	public int send(NodeIDType id, InterfaceRequest message) throws IOException;
}
