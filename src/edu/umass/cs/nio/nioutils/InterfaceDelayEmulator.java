package edu.umass.cs.nio.nioutils;

import edu.umass.cs.nio.InterfaceNodeConfig;

/**
 * @author arun
 *
 * @param <NodeIDType>
 */
public interface InterfaceDelayEmulator<NodeIDType> extends InterfaceNodeConfig<NodeIDType> {

	/**
	 * @param node2
	 * @return The emulated delay in milliseconds to node2.
	 */
	public long getEmulatedDelay(NodeIDType node2);
}
