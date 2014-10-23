/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver.gnamed;

import edu.umass.cs.gns.clientsupport.AccountAccess;
import edu.umass.cs.gns.clientsupport.CommandResponse;
import edu.umass.cs.gns.clientsupport.FieldAccess;
import edu.umass.cs.gns.main.GNS;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.xbill.DNS.ARecord;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Flags;
import org.xbill.DNS.Header;
import org.xbill.DNS.Message;
import org.xbill.DNS.Name;
import org.xbill.DNS.Opcode;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.Record;
import org.xbill.DNS.Section;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.Type;

/**
 *
 * @author westy
 */
public class NameResolution {

  public static final boolean debuggingEnabled = true;

  /** 
   * Sends the query to the DNS server.
   * 
   * @param dnsServer
   * @param query
   * @return A message with either a good response or an error.
   */
  public static Message forwardToDnsServer(SimpleResolver dnsServer, Message query) {
    try {
      Message dnsResponse = dnsServer.send(query);
      if (debuggingEnabled) {
        GNS.getLogger().fine("DNS response " + Rcode.string(dnsResponse.getHeader().getRcode()) + " with "
                + dnsResponse.getSectionArray(Section.ANSWER).length + " answer, "
                + dnsResponse.getSectionArray(Section.AUTHORITY).length + " authoritative and "
                + dnsResponse.getSectionArray(Section.ADDITIONAL).length + " additional records");
      }
      if (isReasonableResponse(dnsResponse)) {
        if (debuggingEnabled) {
          GNS.getLogger().fine("Outgoing response from DNS: " + dnsResponse.toString());
        }
        return dnsResponse;
      }
    } catch (IOException e) {
      GNS.getLogger().warning("DNS resolution failed for " + query + ": " + e);
    }
    return errorMessage(query, Rcode.NXDOMAIN);
  }

  /**
   * Sends the query to the GNS server.
   * 
   * @param query
   * @return A message with either a good response or an error.
   */
  public static Message forwardToGnsServer(Message query) {
    // check for queries we can't handle
    int type = query.getQuestion().getType();
    // Was the query legitimate or implemented?
    if (!Type.isRR(type) && type != Type.ANY) {
      return errorMessage(query, Rcode.NOTIMP);
    }

    // extract the domain (guid) and field from the query
    final String fieldName = Type.string(query.getQuestion().getType());
    final Name requestedName = query.getQuestion().getName();
    final String domainName = requestedName.toString();
    if (debuggingEnabled) {
      GNS.getLogger().fine("Trying GNS lookup for field " + fieldName + " in domain " + domainName);
    }

    String guid = AccountAccess.lookupGuid(domainName);

    if (guid == null) {
      if (debuggingEnabled) {
        GNS.getLogger().fine("GNS lookup: Domain " + domainName + " not found, returning NXDOMAIN result.");
      }
      return errorMessage(query, Rcode.NXDOMAIN);
    }

    CommandResponse fieldResponse = FieldAccess.lookup(guid, fieldName, null, null, null, null);
    if (fieldResponse.isError()) {
      if (debuggingEnabled) {
        GNS.getLogger().fine("GNS lookup: Field " + fieldName + " in domain " + domainName + " not found, returning NXDOMAIN result.");
      }
      return errorMessage(query, Rcode.NXDOMAIN);
    }
    final String ip = fieldResponse.getReturnValue();
    if (debuggingEnabled) {
      GNS.getLogger().fine("Returning A Record with IP " + ip + " for " + requestedName);
    }
    // we'll need to change this to return other record types
    ARecord gnsARecord;
    try {
      gnsARecord = new ARecord(requestedName, DClass.IN, 60, InetAddress.getByName(ip));
    } catch (UnknownHostException e) {
      return errorMessage(query, Rcode.NXDOMAIN);
    }

    Message response = new Message(query.getHeader().getID());
    response.getHeader().setFlag(Flags.QR);
    if (query.getHeader().getFlag(Flags.RD)) {
      response.getHeader().setFlag(Flags.RA);
    }
    response.addRecord(query.getQuestion(), Section.QUESTION);
    response.getHeader().setFlag(Flags.AA);

    // Write the response
    response.addRecord(gnsARecord, Section.ANSWER);
    if (debuggingEnabled) {
      GNS.getLogger().fine("Outgoing response from GNS: " + response.toString());
    }
    return response;
  }

  /**
   * Returns a Message with and error in it if the query is not good.
   *
   * @param query
   * @return
   */
  public static Message checkForErroneousQueries(Message query) {
    Header header = query.getHeader();
    // if there is an error we return an error
    if (header.getRcode() != Rcode.NOERROR) {
      return errorMessage(query, Rcode.FORMERR);
    }
    // we also don't support any weird operations
    if (header.getOpcode() != Opcode.QUERY) {
      return errorMessage(query, Rcode.NOTIMP);
    }
    return null;
  }

  /**
   * Returns true if the response looks ok.
   * Checks for errors and also 0 length answers.
   *
   * @param dnsResponse
   * @return
   */
  public static boolean isReasonableResponse(Message dnsResponse) {
    Integer dnsRcode = null;
    if (dnsResponse != null) {
      dnsRcode = dnsResponse.getHeader().getRcode();
    }
    // If DNS resolution returned something useful return that
    if (dnsRcode != null && dnsRcode == Rcode.NOERROR
            // no error and some useful return value
            && (dnsResponse.getSectionArray(Section.ANSWER).length > 0
            || dnsResponse.getSectionArray(Section.AUTHORITY).length > 0)) {
      // other things we could check for, but do we need to?
      //( && dnsRcode != Rcode.NXDOMAIN && dnsRcode != Rcode.SERVFAIL)
      return true;
    } else {
      return false;
    }
  }


  /**
   * Forms an error message from an incoming packet.
   * 
   * @param in
   * @return the error message
   */
  public static Message formErrorMessage(byte[] in) {
    Header header;
    try {
      header = new Header(in);
    } catch (IOException e) {
      return null;
    }
    return buildErrorMessage(header, Rcode.FORMERR, null);
  }

  /**
   * Forms an error message from a query and response code.
   * 
   * @param query
   * @param rcode
   * @return the error message
   */
  public static Message errorMessage(Message query, int rcode) {
    return buildErrorMessage(query.getHeader(), rcode, query.getQuestion());
  }
  
  /**
   * Creates an error message from a header, response code and a record.
   * @param header
   * @param rcode
   * @param question
   * @return the error message
   */
  private static Message buildErrorMessage(Header header, int rcode, Record question) {
    Message response = new Message();
    response.setHeader(header);
    for (int i = 0; i < 4; i++) {
      response.removeAllRecords(i);
    }
    if (rcode == Rcode.SERVFAIL) {
      response.addRecord(question, Section.QUESTION);
    }
    header.setRcode(rcode);
    return response;
  }

  /**
   * Builds a pretty string showing the query and response. 
   * Used for debugging purposes.
   * 
   * @param query
   * @param response
   * @return 
   */
  public static String queryAndResponseToString(Message query, Message response) {
    StringBuilder result = new StringBuilder();
    result.append(query.getQuestion().getName());
    result.append(", type: ");
    result.append(Type.string(query.getQuestion().getType()));
    result.append(" -> ");

    if (response.getHeader().getRcode() == Rcode.NOERROR) {
      Record[] records = response.getSectionArray(Section.ANSWER);
      if (records.length > 0) {
        Record record = records[0];
        if (record instanceof ARecord) {
          ARecord aRecord = (ARecord) record;
          result.append(aRecord.getAddress().getHostAddress());
        } else {
          result.append("<NOT AN A RECORD>");
        }
      } else {
        result.append("<NO ANSWER SECTIONS>");
      }
    } else {
      result.append(Rcode.string(response.getHeader().getRcode()));
    }
    return result.toString();
  }

}
