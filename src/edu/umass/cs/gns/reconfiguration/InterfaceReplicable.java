package edu.umass.cs.gns.reconfiguration;

// ANYTHING THAT IS REPLICABLE SHOULD BE InterfaceReplicable
/**
@author V. Arun
 */
public interface InterfaceReplicable extends InterfaceApplication {
	public boolean handleRequest(InterfaceRequest request, boolean doNotReplyToClient);
	public String getState(String name, int epoch);
	public boolean updateState(String name, String state);
}
