package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.reconfiguration.InterfaceReplicable;

/**
@author V. Arun
 */

public interface Replicable extends Application, InterfaceReplicable {
	
	// Application.handleDecision will soon not take the third argument
        @Override
	public boolean handleDecision(String name, String value, boolean doNotReplyToClient);  

	public String getState(String name);

        @Override
	public boolean updateState(String name, String state);

}
