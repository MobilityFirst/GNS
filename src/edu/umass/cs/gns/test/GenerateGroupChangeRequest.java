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
public class GenerateGroupChangeRequest<NodeIDType> extends TimerTask {

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

  private NodeIDType selectReplicaController(String name) {
    Set replicaControllers = ConsistentHashing.getReplicaControllerSet(name);
    return (NodeIDType) LocalNameServer.getGnsNodeConfig().getClosestServer(replicaControllers, null);
  }
}

