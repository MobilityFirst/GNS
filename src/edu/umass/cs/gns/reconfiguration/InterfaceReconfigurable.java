package edu.umass.cs.gns.reconfiguration;

// ANYTHING THAT IS RECONFIGURABLE SHOULD BE InterfaceReconfigurable

/**
@author V. Arun
 */
public interface InterfaceReconfigurable extends InterfaceApplication {
	
        // This method was stop, but now we hand off the request to the app.
	public InterfaceReconfigurableRequest getStopRequest(String name, int epoch);
	
	public String getFinalState(String name, int epoch);

	public void putInitialState(String name, int epoch, String state);

	public boolean deleteFinalState(String name, int epoch);
	
	public Integer getEpoch(String name);
}
