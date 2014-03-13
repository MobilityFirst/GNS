package edu.umass.cs.gns.nio;

import org.json.JSONObject;

import java.io.IOException;
import java.util.Set;

/**
 * This interface exists to easily switch between old and new nio packages, both of which support
 * these public send methods. We can remove it later when we have completed testing of the new NIO package.
 *
 *
 * Created by abhigyan on 3/13/14.
 */
public interface GNSNIOTransportInterface extends Runnable {

  public int sendToIDs(Set<Integer> destIDs, JSONObject jsonData) throws IOException;

  public int sendToIDs(short[] destIDs, JSONObject jsonData) throws IOException;

  public int sendToIDs(short[]destIDs, JSONObject jsonData, int excludeID) throws IOException;

  public int sendToIDs(Set<Integer> destIDs, JSONObject jsonData, int excludeID) throws IOException;

  public int sendToID(int id, JSONObject jsonData) throws IOException;

}
