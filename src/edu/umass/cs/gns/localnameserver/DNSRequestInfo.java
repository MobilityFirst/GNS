/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.DNSRecordType;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.util.NSResponseCode;
import org.json.JSONException;
import org.json.JSONObject;



/**
 * Stores information about name lookups transmitted by the local name server.
 * Extends the <code>RequestInfo</code> class.
 *
 * @author abhigyan
 *************************************************************/
public class DNSRequestInfo extends RequestInfo{

  private DNSPacket incomingPacket;

  private int nameserverID;

  private boolean cacheHit = false;

  /**************************************************************
   * Constructs a QueryInfo object with the following parameters
   * @param lnsReqId Query id
   * @param name Host/Domain name
   * @param time System time when query was transmitted
   * @param nameserverID Response name server ID
   **************************************************************/
  public DNSRequestInfo(int lnsReqId, String name, long time,
          int nameserverID, DNSPacket incomingPacket) {
    this.requestType = Packet.PacketType.DNS;
    this.lnsReqID = lnsReqId;
    this.name = name;
    this.startTime = time;

    this.incomingPacket = incomingPacket;
    this.numLookupActives = 0;
    this.nameserverID = nameserverID;
  }

  public synchronized void setNameserverID(int nameserverID1) {
    nameserverID = nameserverID1;
  }

  /**
   * @return the incomingPacket
   */
  public synchronized DNSPacket getIncomingPacket() {
    return incomingPacket;
  }

  @Override
  public synchronized String getLogString() {
    StringBuilder str = new StringBuilder();
    str.append(isSuccess() ? "Success-Lookup": "Failed-Lookup");
    if(isCacheHit()) str.append("CacheHit");
    str.append("\t");
    str.append("0\t");
    str.append(incomingPacket.getKey() + "\t");
    str.append(name);
    str.append("\t" + getResponseLatency());
    str.append("\t0");
    str.append("\t0");
    str.append("\t0");
    str.append("\t0");
    str.append("\t" + nameserverID);
    str.append("\t" + LocalNameServer.getNodeID());
    str.append("\t" + startTime);
    str.append("\t" + numLookupActives);
    str.append("\t[]");
    str.append("\t");
    str.append(getEventCodesString());
    return str.toString();
  }

  @Override
  public synchronized JSONObject getErrorMessage() {
    try {
      DNSPacket dnsPacketOut = new DNSPacket(incomingPacket.toJSONObjectQuestion());
      dnsPacketOut.getHeader().setResponseCode(NSResponseCode.ERROR);
      dnsPacketOut.getHeader().setQRCode(DNSRecordType.RESPONSE);
      return dnsPacketOut.toJSONObject();
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON: " + e);
    }
    return null;
  }

  public synchronized boolean isCacheHit() {
    return cacheHit;
  }

  public synchronized void setCacheHit(boolean cacheHit) {
    this.cacheHit = cacheHit;
  }
}
