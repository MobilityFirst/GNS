package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.nsdesign.Replicable;

/**
 * This interface must be implemented by the system (e.g. GNS) to use paxos.
 * User: abhigyan
 * Date: 6/29/13
 * Time: 8:31 PM
 */
public interface PaxosInterface extends Replicable {
	public void stop(String name, String value);
}
