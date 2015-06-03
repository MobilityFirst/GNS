package edu.umass.cs.nio;

import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author V. Arun, Abhigyan
 * 
 * This interface exists to easily switch between old and new nio packages, both of which support
 * this public send method. We can remove it later when we have completed testing of the new NIO package.
 */
public interface InterfaceJSONNIOTransport<NodeIDType> {

  public int sendToID(NodeIDType id, JSONObject jsonData) throws IOException;
  
  public int sendToAddress(InetSocketAddress isa, JSONObject jsonData) throws IOException;
  
  public void addPacketDemultiplexer(AbstractPacketDemultiplexer pd);  
  
  public NodeIDType getMyID();

  public void stop();
}
