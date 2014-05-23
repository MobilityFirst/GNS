package edu.umass.cs.gns.test;

import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.nsdesign.packet.NewActiveProposalPacket;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.json.JSONException;

import java.util.Set;
import java.util.TimerTask;

/**
 * Created by abhigyan on 5/21/14.
 */
public class GenerateGroupChangeRequest extends TimerTask {

  private int requestCount;
  private String name;
  private LNSPacketDemultiplexer packetDemultiplexer;
  private TestGroupChangeRequest groupChangeRequest;

  public GenerateGroupChangeRequest(String name, int count, TestGroupChangeRequest grpChange,
                                    LNSPacketDemultiplexer packetDemultiplexer) {
    this.requestCount = count;
    this.name = name;
    this.packetDemultiplexer = packetDemultiplexer;
    this.groupChangeRequest = grpChange;
  }

  @Override
  public void run() {

    NewActiveProposalPacket packet = new NewActiveProposalPacket(name, selectReplicaController(name),
            groupChangeRequest.replicaSet, groupChangeRequest.version);
    try {
      packetDemultiplexer.handleJSONObject(packet.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  private int selectReplicaController(String name) {
    Set<Integer> replicaControllers = ConsistentHashing.getReplicaControllerSet(name);
    return LocalNameServer.getGnsNodeConfig().getClosestServer(replicaControllers, null);
//      ArrayList<Integer> replicaControllers = new ArrayList<Integer>(ConsistentHashing.getReplicaControllerSet(name));
//      return replicaControllers.get(new Random().nextInt(replicaControllers.size()));
  }
}

