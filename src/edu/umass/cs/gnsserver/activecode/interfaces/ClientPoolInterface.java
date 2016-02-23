package edu.umass.cs.gnsserver.activecode.interfaces;

import edu.umass.cs.gnsserver.activecode.ActiveCodeClient;

/**
 * @author gaozy
 *
 */
public interface ClientPoolInterface {
	/**
	 * @param threadID
	 * @return Client corresponding to threadID as registered by ActiveCodeThreadFactory
	 */
	public ActiveCodeClient getClient(long threadID);
}
