package edu.umass.cs.nio;

import java.net.InetAddress;
import java.util.Set;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            An interface to translate from integer IDs to socket addresses.
 */
public interface InterfaceNodeConfig<NodeIDType> extends
		Stringifiable<NodeIDType> {

	/**
	 * @param id
	 * @return Whether the node id exists in the node config.
	 */
	public abstract boolean nodeExists(NodeIDType id);

	/**
	 * @param id
	 * @return InetAddress corresponding to {@code id}.
	 */
	public abstract InetAddress getNodeAddress(NodeIDType id);
        
        /**
	 * @param id
	 * @return Locally bindable InetAddress corresponding to {@code id}.
	 */
        public abstract InetAddress getBindAddress(NodeIDType id);

	/**
	 * @param id
	 * @return Port number corresponding to {@code id}.
	 */
	public abstract int getNodePort(NodeIDType id);

	/**
	 * @return Set of all node IDs. Avoid using this method or at least avoid
	 *         reusing the result of this method if the underlying set of nodes
	 *         can change.
	 */
	public abstract Set<NodeIDType> getNodeIDs();

}
