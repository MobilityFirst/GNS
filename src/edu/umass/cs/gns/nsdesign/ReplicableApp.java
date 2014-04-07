package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nsdesign.replicaController.Application;

/**
@author V. Arun
 */
public class ReplicableApp implements Replicable {
	Application app;
	
	public ReplicableApp(Application app) {
		this.app = app;
	}
	private void assertReplicable() {
		if(!(this.app instanceof Replicable)) throw new RuntimeException("Attempting to replicate an application that is not replicable");
	}

	@Override
	public boolean handleDecision(String name, String value, boolean recovery) {
		assertReplicable();
		return ((Replicable)this.app).handleDecision(name, value, recovery);
	}

	@Override
	public String getState(String name) {
		assertReplicable();
		return ((Replicable)this.app).getState(name);
	}

	@Override
	public boolean updateState(String name, String state) {
		assertReplicable();
		return ((Replicable)this.app).updateState(name, state);
	}
}
