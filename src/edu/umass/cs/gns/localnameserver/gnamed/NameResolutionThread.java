/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver.gnamed;

import edu.umass.cs.gns.clientsupport.AccountAccess;
import edu.umass.cs.gns.clientsupport.CommandResponse;
import edu.umass.cs.gns.clientsupport.FieldAccess;
import edu.umass.cs.gns.main.GNS;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.util.logging.Level;

import org.xbill.DNS.ARecord;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Flags;
import org.xbill.DNS.Header;
import org.xbill.DNS.Message;
import org.xbill.DNS.Name;
import org.xbill.DNS.OPTRecord;
import org.xbill.DNS.Opcode;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.Record;
import org.xbill.DNS.Section;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.Type;

//import edu.umass.cs.gns.client.UniversalGnsClient;
/**
 * This class defines a NameResolutionThread.
 *
 * @author <a href="mailto:manu@frogthinker.org">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class NameResolutionThread extends Thread {

  private final SimpleResolver dnsServer;
  private final DatagramSocket sock;
  private final DatagramPacket incomingPacket;
  private final byte[] incomingData;
  //
  private final boolean debuggingEnabled = true;

  /**
   * Creates a new <code>NameResolutionThread</code> object
   *
   * @param socket
   * @param incomingPacket
   * @param incomingData
   * @param dnsServer
   */
  public NameResolutionThread(DatagramSocket socket, DatagramPacket incomingPacket, byte[] incomingData, SimpleResolver dnsServer) {
    this.sock = socket;
    this.incomingPacket = incomingPacket;
    this.incomingData = incomingData;
    this.dnsServer = dnsServer;
  }

  /**
   * @see java.lang.Thread#run()
   */
  @Override
  public void run() {
    Message query;
    byte[] response = null;
    try {
      query = new Message(incomingData);

      response = generateReply(query, incomingData, incomingPacket.getLength(), null);
      if (response == null) { // means we don't need to do anything
        return;
      }
    } catch (IOException e) {
      response = formErrorMessage(incomingData);
    }
    DatagramPacket outgoingPacket = new DatagramPacket(response, response.length, incomingPacket.getAddress(), incomingPacket.getPort());
    try {
      synchronized (sock) {
        sock.send(outgoingPacket);
      }
    } catch (IOException e) {
      GNS.getLogger().severe("Failed to send response" + e);
    }
  }

  /*
   * Note: a null return value means that the caller doesn't need to do
   * anything. Currently this only happens if this is an AXFR request over TCP.
   */
  private byte[] generateReply(Message query, byte[] in, int length, Socket s) {
    Header header;
    int maxLength;
    if (debuggingEnabled) {
      GNS.getLogger().info("Incoming request: " + query.toString());
    }

    header = query.getHeader();
    // if it's not a query we just ignore it
    if (header.getFlag(Flags.QR)) {
      return null;
    }

    // if there is an error we return an error
    if (header.getRcode() != Rcode.NOERROR) {
      return errorMessage(query, Rcode.FORMERR);
    }

    // we also don't support any weird operations
    if (header.getOpcode() != Opcode.QUERY) {
      return errorMessage(query, Rcode.NOTIMP);
    }

    // not sure what's going on here other than setting the max length of the returned records
    if (s != null) {
      maxLength = 65535;
    } else if (query.getOPT() != null) {
      maxLength = Math.max(query.getOPT().getPayloadSize(), 512);
    } else {
      maxLength = 512;
    }

    // Forwarding to DNS Server
    Message dnsResponse = null;
    Integer dnsRcode = null;
    try {
      dnsResponse = dnsServer.send(query);

      if (debuggingEnabled) {
        GNS.getLogger().info("DNS response " + Rcode.string(dnsResponse.getHeader().getRcode()) + " with "
                + dnsResponse.getSectionArray(Section.ANSWER).length + " answer, "
                + dnsResponse.getSectionArray(Section.AUTHORITY).length + " authoritative and "
                + dnsResponse.getSectionArray(Section.ADDITIONAL).length + " additional records");
      }
      dnsRcode = dnsResponse.getHeader().getRcode();
    } catch (IOException e) {
      GNS.getLogger().log(Level.WARNING, "DNS resolution failed for " + query, e);
    }

    // if DNS resolution returned something useful return that
    if (dnsRcode != null && dnsRcode == Rcode.NOERROR
            // no error and some useful return value
            && (dnsResponse.getSectionArray(Section.ANSWER).length > 0
            || dnsResponse.getSectionArray(Section.AUTHORITY).length > 0)) {
      // other things we could check for, but do we need to?
      //( && dnsRcode != Rcode.NXDOMAIN && dnsRcode != Rcode.SERVFAIL)
      if (debuggingEnabled) {
        GNS.getLogger().info("Outgoing response: " + dnsResponse.toString());
      }
      return dnsResponse.toWire(maxLength);
      //
    } else { // DNS resolution failed, let's try the GNS

      final String fieldName = Type.string(query.getQuestion().getType());
      final Name requestedName = query.getQuestion().getName();
      final String domainName = requestedName.toString();
      GNS.getLogger().info("DNS resolution failed. Trying GNS lookup for field " + fieldName + " in domain " + domainName);

      String guid = AccountAccess.lookupGuid(domainName);
      try {
        if (guid == null) {
          if (debuggingEnabled) {
            GNS.getLogger().info("GNS lookup: Domain " + domainName + " not found, returning NXDOMAIN result.");
          }
          return errorMessage(query, Rcode.NXDOMAIN);
        }

        CommandResponse fieldResponse = FieldAccess.lookup(guid, fieldName, null, null, null, null);
        if (fieldResponse.isError()) {
          if (debuggingEnabled) {
            GNS.getLogger().info("GNS lookup: Field " + fieldName + " in domain " + domainName + " not found, returning NXDOMAIN result.");
          }
          return errorMessage(query, Rcode.NXDOMAIN);
        }
        final String ip = fieldResponse.getReturnValue();
        if (debuggingEnabled) {
          GNS.getLogger().info("Returning A Record with IP " + ip + " for " + requestedName);
        }
        ARecord gnsARecord = new ARecord(requestedName, DClass.ANY, 0, InetAddress.getByName(ip));

        Message response = new Message(query.getHeader().getID());
        response.getHeader().setFlag(Flags.QR);
        if (query.getHeader().getFlag(Flags.RD)) {
          response.getHeader().setFlag(Flags.RD);
        }
        response.addRecord(query.getQuestion(), Section.QUESTION);
        response.getHeader().setFlag(Flags.AA);

        int type = query.getQuestion().getType();
        // Was the query legitimate or implemented?
        if (!Type.isRR(type) && type != Type.ANY) {
          return errorMessage(query, Rcode.NOTIMP);
        }

        // Write the response
        response.addRecord(gnsARecord, Section.ANSWER);

        if (debuggingEnabled) {
          GNS.getLogger().info("Outgoing response from GNS: " + response.toString());
        }

        final byte[] wireBytes = response.toWire(maxLength);
        GNS.getLogger().info("Wire bytes: " + wireBytes.length);

        return wireBytes;
      } catch (Exception e) {
        GNS.getLogger().log(Level.WARNING, "GNS resolution failed for " + query, e);
        return errorMessage(query, Rcode.NXDOMAIN);
      }
    }
  }

  private byte[] buildErrorMessage(Header header, int rcode, Record question) {
    Message response = new Message();
    response.setHeader(header);
    for (int i = 0; i < 4; i++) {
      response.removeAllRecords(i);
    }
    if (rcode == Rcode.SERVFAIL) {
      response.addRecord(question, Section.QUESTION);
    }
    header.setRcode(rcode);
    return response.toWire();
  }

  private byte[] formErrorMessage(byte[] in) {
    Header header;
    try {
      header = new Header(in);
    } catch (IOException e) {
      return null;
    }
    return buildErrorMessage(header, Rcode.FORMERR, null);
  }

  private byte[] errorMessage(Message query, int rcode) {
    return buildErrorMessage(query.getHeader(), rcode, query.getQuestion());
  }

}
