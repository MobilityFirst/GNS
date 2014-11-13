package edu.umass.cs.gns.nio;

import java.net.InetAddress;
import java.util.Set;

import edu.umass.cs.gns.util.Stringifiable;

/**
 * @author V. Arun
 */

/* An interface to translate from integer IDs to socket addresses.
 * 
 */
public interface InterfaceNodeConfig<NodeIDType> extends Stringifiable<NodeIDType>{

    public abstract boolean nodeExists(NodeIDType id);

    public abstract InetAddress getNodeAddress(NodeIDType id);

    public abstract int getNodePort(NodeIDType id);

    /* FIXME: This method needs to be made available only in 
     * "InterfaceDynamicNodeConfig". 
     */
    public abstract Set<NodeIDType> getNodeIDs();

}

