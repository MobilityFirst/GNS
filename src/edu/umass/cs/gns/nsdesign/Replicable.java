package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nsdesign.replicaController.Application;

/**
 * Created by abhigyan on 3/27/14.
 */
/* PaxosInterface is the same as this interface.
 */
public interface Replicable extends Application {

  //public void handleDecision(String name, String value, boolean recovery);

  public String getState(String name);

  public boolean updateState(String name, String state);
}
