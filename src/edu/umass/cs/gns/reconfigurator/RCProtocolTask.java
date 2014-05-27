package edu.umass.cs.gns.reconfigurator;
/**
@author V. Arun
 */
public interface RCProtocolTask extends Runnable {
	public void setReplicaController(ReplicaController rc); // FIXME: replace with only what's needed.
}
