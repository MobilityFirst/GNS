package edu.umass.cs.gns.nio;

import java.net.InetAddress;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 6/29/13
 * Time: 7:45 PM
 * To change this template use File | Settings | File Templates.
 */


/* An interface to translate from integere IDs to socket addresses.
 * 
 */
public interface NodeConfig {

    public abstract boolean containsNodeInfo(int ID);

    public abstract int getNodeCount();

    public abstract InetAddress getNodeAddress(int ID);

    public abstract int getNodePort(int ID);


}

