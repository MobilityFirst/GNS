package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nsdesign.replicaController.Application;

import java.util.Set;

/**
 * Created by abhigyan on 3/27/14.
 */
public interface Reconfigurable extends Application {

	public void stopVersion(String name, int version);

	public String getFinalState(String name, int version);

	public void putInitialState(String name, int version, String state, Set<Integer> activeReplicas);

	public int deleteFinalState(String name, int version);

 //	public int[] getCurrentOldVersions(String name);

}
