package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by abhigyan on 2/23/14.
 * @deprecated
 */
public class UpdateStatus {

  private String name;
  private int localNameServerID;
  private Set<Integer> allNameServers;
  private Set<Integer> nameServersResponded;
  private ConfirmUpdateLNSPacket confirmUpdate;

  public UpdateStatus(String name, int localNameServerID, Set<Integer> allNameServers,
                      ConfirmUpdateLNSPacket confirmUpdate) {
    this.name = name;
    this.localNameServerID = localNameServerID;
    this.allNameServers = allNameServers;
    nameServersResponded = new HashSet<Integer>();
    this.confirmUpdate = confirmUpdate;
  }

  public ConfirmUpdateLNSPacket getConfirmUpdateLNSPacket() {
    return confirmUpdate;
  }

  public Set<Integer> getAllNameServers() {
    return allNameServers;
  }

  public int getLocalNameServerID() {
    return localNameServerID;
  }

  public String getName() {
    return name;
  }

  public void addNameServerResponded(int nameServerID) {
    nameServersResponded.add(nameServerID);
  }

  public boolean haveMajorityNSSentResponse() {
    GNS.getLogger().fine("All ns size:" + allNameServers.size());
    GNS.getLogger().fine("Responded ns size:" + nameServersResponded.size());
    if (allNameServers.size() == 0) {
      return true;
    }
    if (nameServersResponded.size() * 2 > allNameServers.size()) {
      return true;
    }
    return false;
  }
}
