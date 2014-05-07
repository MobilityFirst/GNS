package edu.umass.cs.gns.nio;

import org.json.JSONObject;

import java.io.IOException;

/**
 * This interface exists to easily switch between old and new nio packages, both of which support
 * these public send methods. We can remove it later when we have completed testing of the new NIO package.
 *
 *
 * Created by abhigyan on 3/13/14.
 */
public interface GNSNIOTransportInterface{

  public int sendToID(int id, JSONObject jsonData) throws IOException;

}
