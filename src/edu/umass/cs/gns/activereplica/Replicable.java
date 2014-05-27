package edu.umass.cs.gns.activereplica;

/**
@author V. Arun
 */

public interface Replicable {

	public boolean handleDecision(String name, String value, boolean doNotReplyToClient);

	public String getState(String name);

	public boolean updateState(String name, String state);

}
