/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gns.nsdesign.nodeconfig;

import java.net.InetAddress;
import java.util.Set;

/**
 * An interface to translate from integere IDs to socket addresses.
 * 
 * @author Abhigyan Sharma, V. Arun
 * History:
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 6/29/13
 * Time: 7:45 PM
 * To change this template use File | Settings | File Templates.
 * @param <NodeIDType>
 */
public interface InterfaceNodeConfig<NodeIDType> {

    public abstract boolean nodeExists(NodeIDType id);

    public abstract Set<NodeIDType> getNodeIDs();

    public abstract InetAddress getNodeAddress(NodeIDType id);

    public abstract int getNodePort(NodeIDType id);


}

