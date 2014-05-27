package edu.umass.cs.gns.activereplica;
/**
@author V. Arun
 */
public interface ARProtocolTask extends Runnable{
	public void setActiveReplica(ActiveReplica<?> activeReplica);
}
