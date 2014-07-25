package edu.umass.cs.gns.nio;

import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * This interface exists to easily switch between old and new nio packages, both of which support
 * this public send method. We can remove it later when we have completed testing of the new NIO package.
 *
 *
 * Created by abhigyan on 3/13/14.
 */
public interface InterfaceJSONNIOTransport{

  public int sendToID(int id, JSONObject jsonData) throws IOException;
  
  public int sendToAddress(InetSocketAddress isa, JSONObject jsonData) throws IOException;
  
  public int getMyID();

  public void stop();
}
