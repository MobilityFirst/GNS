/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport;

import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.packet.DNSPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.DNSRecordType;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.nio.interfaces.Stringifiable;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Stores information about name lookups transmitted by the local name server.
 * Extends the <code>RequestInfo</code> class.
 *
 * @author abhigyan
 * @param <NodeIDType>
 ************************************************************
 */
public class DNSRequestInfo<NodeIDType> extends RequestInfo {

  private final DNSPacket<NodeIDType> incomingPacket;

  private int nameserverID;

  private final Stringifiable<NodeIDType> unstringer;

  /**
   * Constructs a DNSRequestInfo object with the following parameters
   *
   * @param lnsReqId Query id
   * @param name Host/Domain name
   * @param nameserverID Response name server ID
   * @param incomingPacket
   * @param unstringer
   */
  public DNSRequestInfo(int lnsReqId, String name, int nameserverID, DNSPacket<NodeIDType> incomingPacket,
          Stringifiable<NodeIDType> unstringer) {
    this.requestType = Packet.PacketType.DNS;
    this.ccpReqID = lnsReqId;
    this.name = name;
    this.startTime = System.currentTimeMillis();
    this.incomingPacket = incomingPacket;
    this.numLookupActives = 0;
    this.nameserverID = nameserverID;
    this.unstringer = unstringer;
  }

  /**
   * Set the nameserverID.
   *
   * @param nameserverID
   */
  public synchronized void setNameserverID(int nameserverID) {
    this.nameserverID = nameserverID;
  }

  /**
   * Return the incoming packet.
   *
   * @return the incomingPacket
   */
  public synchronized DNSPacket<NodeIDType> getIncomingPacket() {
    return incomingPacket;
  }

  @Override
  public synchronized JSONObject getErrorMessage() {
    try {
      DNSPacket<NodeIDType> dnsPacketOut = new DNSPacket<NodeIDType>(incomingPacket.toJSONObjectQuestion(), unstringer);
      dnsPacketOut.getHeader().setResponseCode(NSResponseCode.FAIL_ACTIVE_NAMESERVER);
      dnsPacketOut.getHeader().setQueryResponseCode(DNSRecordType.RESPONSE);
      return dnsPacketOut.toJSONObject();
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON: " + e);
    }
    return null;
  }
}
