package edu.umass.cs.nio;

import java.net.InetAddress;
import java.util.Set;

/**
 * @author V. Arun
 * @param <NodeIDType> 
 */

/* An interface to translate from integer IDs to socket addresses.
 * 
 */
public interface InterfaceNodeConfig<NodeIDType> extends Stringifiable<NodeIDType>{

    public abstract boolean nodeExists(NodeIDType id);

    public abstract InetAddress getNodeAddress(NodeIDType id);

    public abstract int getNodePort(NodeIDType id);

    public abstract Set<NodeIDType> getNodeIDs();
   
}

