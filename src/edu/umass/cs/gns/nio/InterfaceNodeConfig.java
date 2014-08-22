package edu.umass.cs.gns.nio;

import java.net.InetAddress;
import java.util.Set;

/**
 * @author Abhigyan Sharma, V. Arun
 * History:
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 6/29/13
 * Time: 7:45 PM
 * To change this template use File | Settings | File Templates.
 */


/* An interface to translate from integere IDs to socket addresses.
 * 
 */
public interface InterfaceNodeConfig<NodeIDType> {

    public abstract boolean containsNodeInfo(NodeIDType id);
    
    public abstract Set<NodeIDType> getNodeIDs();

    public abstract InetAddress getNodeAddress(NodeIDType id);

    public abstract int getNodePort(NodeIDType id);


}

