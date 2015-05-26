/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gns.test;

import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.CCPPacketDemultiplexerV1;
import edu.umass.cs.gns.newApp.packet.AddRecordPacket;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;

import java.util.Set;
import java.util.TimerTask;

/**
 * Created by abhigyan on 5/21/14.
 */
@SuppressWarnings("unchecked")
class GenerateAddRequest<NodeIDType> extends TimerTask {

  private int requestCount;
  private String name;
  private CCPPacketDemultiplexerV1 packetDemux;
  private int objectSizeKB;
  private int ttl;

  private Set<NodeIDType> activeNameServers;

  public GenerateAddRequest(String name, int count, int objectSizeBytes, int ttl, CCPPacketDemultiplexerV1 packetDemux,
          Set<NodeIDType> activeNameServers) {
    this.requestCount = count;
    this.name = name;
    this.packetDemux = packetDemux;
    this.objectSizeKB = objectSizeBytes;
    this.ttl = ttl;
    this.activeNameServers = activeNameServers;
  }

  public GenerateAddRequest(String name, int count, int objectSizeBytes, int ttl, CCPPacketDemultiplexerV1 packetDemux) {
    this(name, count, objectSizeBytes, ttl, packetDemux, null);
  }

  @Override
  public void run() {

    ResultValue newValue = new ResultValue();
    newValue.add(Util.randomString(objectSizeKB));
    AddRecordPacket packet = new AddRecordPacket(null, requestCount, name, "EdgeRecord", newValue, null, ttl);

    if (activeNameServers != null) {
      packet.setActiveNameServers(activeNameServers);
    }

    try {
      packetDemux.handleJSONObject(packet.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

}
