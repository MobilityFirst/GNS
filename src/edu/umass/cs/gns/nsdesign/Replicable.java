package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.packet.paxospacket.RequestPacket;

/**
 * Created by abhigyan on 3/27/14.
 */
public interface Replicable {

  public abstract void handleDecision(String name, RequestPacket requestPacket, boolean recovery);

  public abstract String getState(String name);

  public abstract void updateState(String name, String state);

}
