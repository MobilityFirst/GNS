/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gns.test;

import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.packet.NewActiveProposalPacket;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.json.JSONException;

import java.util.Set;
import java.util.TimerTask;

/**
 * Created by abhigyan on 5/21/14.
 * @param <NodeIDType>
 */
@SuppressWarnings("unchecked")
public class GenerateGroupChangeRequest<NodeIDType> extends TimerTask {

  private final int requestCount;
  private final String name;
  private final LNSPacketDemultiplexer packetDemultiplexer;
  private final TestGroupChangeRequest groupChangeRequest;
  private final ClientRequestHandlerInterface<NodeIDType> handler;

  public GenerateGroupChangeRequest(String name, int count, TestGroupChangeRequest grpChange,
                                    LNSPacketDemultiplexer packetDemultiplexer, ClientRequestHandlerInterface<NodeIDType> handler) {
    this.requestCount = count;
    this.name = name;
    this.packetDemultiplexer = packetDemultiplexer;
    this.groupChangeRequest = grpChange;
    this.handler = handler;
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
    return (NodeIDType) handler.getGnsNodeConfig().getClosestServer(replicaControllers, null);
  }
}

