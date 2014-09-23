package edu.umass.cs.gns.reconfiguration;


/**
@author V. Arun
 */
public interface InterfaceReplicable extends InterfaceApplication {
	public boolean handleRequest(InterfaceRequest request, boolean doNotReplyToClient);
	public String getState(String name, int epoch);
	public boolean updateState(String name, String state);
}
