package edu.umass.cs.gns.nsdesign;


/**
@author V. Arun
 */
public class ReconfigurableApp implements Reconfigurable {

	Application app=null;
	
	public ReconfigurableApp(Application app) {
		this.app = app;
	}
	private void assertReconfigurable() {
		if(!(this.app instanceof Reconfigurable)) 
			throw new RuntimeException("Attempting to reconfigure an application that is not reconfigurable");
	}
	private boolean isStopRequest(String value) {
		/* logic to determine if it is a stop request */
		return false;
	}
	@Override
	public boolean handleDecision(String name, String value, boolean recovery) {
		boolean executed = this.app.handleDecision(name, value, recovery);
		if(isStopRequest(value)) executed &= stopVersion(name, (short)-1);
		return executed;
	}

	@Override
	public boolean stopVersion(String name, short version) {
		assertReconfigurable();
		return ((Reconfigurable)(this.app)).stopVersion(name, version);
	}

	@Override
	public String getFinalState(String name, short version) {
		assertReconfigurable();
		return ((Reconfigurable)this.app).getFinalState(name, version);
	}

	@Override
	public void putInitialState(String name, short version, String state) {
		assertReconfigurable();
		((Reconfigurable)this.app).putInitialState(name, version, state);
	}

	@Override
	public int deleteFinalState(String name, short version) {
		assertReconfigurable();
		return ((Reconfigurable)(this.app)).deleteFinalState(name, version);
	}

}
