package edu.umass.cs.gns.nsdesign.activeReconfiguration;

import edu.umass.cs.gns.nsdesign.packet.NewActiveSetStartupPacket;

import java.util.HashSet;

/**
 *
 * Created by abhigyan on 3/28/14.
 * 
 * FIXME: Arun: Unclear what the point of this class is. Needs to be
 * documented. It seems to be waiting for a majority of responses 
 * from (old or new?) active replicas. Should use WaitforUtility in
 * multipaxos.
 * @param <NodeIDType>
 */
public class NewActiveStartInfo<NodeIDType> {

  public NewActiveSetStartupPacket originalPacket;
  private HashSet<NodeIDType> activesResponded = new HashSet<NodeIDType>();
  boolean sent = false;

  public NewActiveStartInfo(NewActiveSetStartupPacket originalPacket) {
    this.originalPacket = originalPacket;
  }

  public synchronized void receivedResponseFromActive(NodeIDType ID) {
    activesResponded.add(ID);
  }

  public synchronized boolean haveMajorityActivesResponded() {

    if (sent == false && activesResponded.size()*2 > originalPacket.getNewActiveNameServers().size()) {
      sent = true;
      return true;
    }
    return false;
  }
}
