package edu.umass.cs.gns.nio;

import java.net.InetAddress;
import java.util.Set;

import edu.umass.cs.gns.util.Stringifiable;

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
    
    /**
     * Returns the administrator port for the given node.
     * 
     * @param id
     * @return 
     */
    public abstract int getAdminPort(NodeIDType id);
    
    /**
     * Returns the ping port for the given node.
     * 
     * @param id
     * @return 
     */
    public abstract int getPingPort(NodeIDType id);
    
    /**
     * Returns the average ping latency to the given node.
     * 
     * @param id
     * @return 
     */
    public long getPingLatency(NodeIDType id);
    
    /** 
     * Stores the average ping latency to the given node.
     * 
     * @param id
     * @param responseTime 
     */
    public void updatePingLatency(NodeIDType id, long responseTime);

    public abstract Set<NodeIDType> getNodeIDs();
    
    /**
     * Returns the version number of the NodeConfig.
     * @return 
     */
    public abstract long getVersion();
    
    //public abstract void register(Runnable callback);

}

