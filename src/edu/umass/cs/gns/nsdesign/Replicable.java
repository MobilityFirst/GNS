package edu.umass.cs.gns.nsdesign;

/**
@author V. Arun
 */

public interface Replicable extends Application {

	public String getState(String name);
  
  public boolean updateState(String name, String state);

}
