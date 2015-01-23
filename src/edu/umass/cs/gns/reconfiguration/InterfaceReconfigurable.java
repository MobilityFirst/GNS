package edu.umass.cs.gns.reconfiguration;

/**
@author V. Arun
 */
public interface InterfaceReconfigurable extends InterfaceApplication {
	
	public InterfaceReconfigurableRequest getStopRequest(String name, int epoch);
	
	public String getFinalState(String name, int epoch);

	public void putInitialState(String name, int epoch, String state);

	public boolean deleteFinalState(String name, int epoch);
	
	public Integer getEpoch(String name);
}
