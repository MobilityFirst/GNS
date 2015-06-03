package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;

import edu.umass.cs.gns.gigapaxos.InterfaceRequest;
import edu.umass.cs.gns.nio.GenericMessagingTask;

/**
@author V. Arun
 */
/* This interface is currently not used. It is meant to be a general 
 * interface for a messenger of which JSONMessenger is a special
 * case. This would be useful is we move away from JSON and NIO,
 * but that seems so unthinkable at this point.
 */
@Deprecated
public interface InterfaceMessenger<NodeIDType> {
	public NodeIDType getMyID();
	public void send(GenericMessagingTask<NodeIDType,InterfaceRequest> mtask) throws IOException, RequestParseException; 
	public int send(NodeIDType id, InterfaceRequest message) throws IOException;
}
