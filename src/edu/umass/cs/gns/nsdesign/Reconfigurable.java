package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurable;

/**
@author V. Arun
 */
public interface Reconfigurable extends Application, InterfaceReconfigurable {

	public boolean stopVersion(String name, short version); // getFinalState should return non-null value after this call

	public String getFinalState(String name, short version);

	public void putInitialState(String name, short version, String state);

	public int deleteFinalState(String name, short version);

}
