package edu.umass.cs.gns.nsdesign.activeReconfiguration;

import edu.umass.cs.gns.nsdesign.packet.NewActiveSetStartupPacket;

import java.util.HashSet;

/**
 *
 * Created by abhigyan on 3/28/14.
 */
public class NewActiveStartInfo {

  public NewActiveSetStartupPacket originalPacket;
  private HashSet<Integer> activesResponded = new HashSet<Integer>();
  boolean sent = false;

  public NewActiveStartInfo(NewActiveSetStartupPacket originalPacket) {
    this.originalPacket = originalPacket;
  }

  public synchronized void receivedResponseFromActive(int ID) {
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
