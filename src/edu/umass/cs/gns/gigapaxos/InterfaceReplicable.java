package edu.umass.cs.gns.gigapaxos;





/**
@author V. Arun
 */
public interface InterfaceReplicable extends InterfaceApplication {
	public boolean handleRequest(InterfaceRequest request, boolean doNotReplyToClient);
	public String getState(String name);
	public boolean updateState(String name, String state);
}
