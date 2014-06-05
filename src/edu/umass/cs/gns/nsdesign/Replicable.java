package edu.umass.cs.gns.nsdesign;

/**
@author V. Arun
 */

public interface Replicable extends Application {
	
	// Application.handleDecision will soon not take the third argument
        @Override
	public boolean handleDecision(String name, String value, boolean doNotReplyToClient);  

	public String getState(String name);

	public boolean updateState(String name, String state);

}
