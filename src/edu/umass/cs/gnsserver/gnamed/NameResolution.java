/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsserver.gnamed;

import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import static edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess.HRN_GUID;
import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.utils.DelayProfiler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.xbill.DNS.ARecord;
import org.xbill.DNS.CNAMERecord;
import org.xbill.DNS.Cache;
import org.xbill.DNS.Credibility;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Flags;
import org.xbill.DNS.Header;
import org.xbill.DNS.Message;
import org.xbill.DNS.Name;
import org.xbill.DNS.Opcode;
import org.xbill.DNS.RRset;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.Record;
import org.xbill.DNS.Section;
import org.xbill.DNS.SetResponse;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.Type;
import org.xbill.DNS.MXRecord;
import org.xbill.DNS.NSRecord;
import org.xbill.DNS.TextParseException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class NameResolution {

  private static final Logger LOG = Logger.getLogger(NameResolution.class.getName());

  /**
   * @return Logger used by most of the client support package.
   */
  public static final Logger getLogger() {
    return LOG;
  }

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
      NameResolution.getLogger().log(Level.FINE,
              "DNS response {0} with {1} answer, {2} authoritative and {3} additional records", new Object[]{Rcode.string(dnsResponse.getHeader().getRcode()), dnsResponse.getSectionArray(Section.ANSWER).length, dnsResponse.getSectionArray(Section.AUTHORITY).length, dnsResponse.getSectionArray(Section.ADDITIONAL).length});

      if (isReasonableResponse(dnsResponse)) {
        NameResolution.getLogger().log(Level.FINE,
                "Outgoing response from DNS: {0}", dnsResponse.toString());
        return dnsResponse;
      }
    } catch (IOException e) {
      NameResolution.getLogger().log(Level.WARNING,
              "DNS resolution failed for {0}: {1}", new Object[]{query, e});
    }
    return errorMessage(query, Rcode.NXDOMAIN);
  }

  /**
   * Sends the query to the GNS server.
   *
   * @param gnsServer
   * @param query
   * @return A message with either a good response or an error.
   */
  public static Message forwardToGnsServer(SimpleResolver gnsServer, Message query) {
    try {
      Message dnsResponse = gnsServer.send(query);
      NameResolution.getLogger().log(Level.FINE,
              "DNS response {0} with {1} answer, {2} authoritative and {3} additional records",
              new Object[]{Rcode.string(dnsResponse.getHeader().getRcode()),
                dnsResponse.getSectionArray(Section.ANSWER).length,
                dnsResponse.getSectionArray(Section.AUTHORITY).length,
                dnsResponse.getSectionArray(Section.ADDITIONAL).length});
      if (isReasonableResponse(dnsResponse)) {
        NameResolution.getLogger().log(Level.FINE, "Outgoing response from DNS: {0}",
                dnsResponse.toString());
        return dnsResponse;
      }
    } catch (IOException e) {
      NameResolution.getLogger().log(Level.WARNING,
              "DNS resolution failed for {0}: {1}", new Object[]{query, e});
    }
    return errorMessage(query, Rcode.NXDOMAIN);
  }

  /**
   * Lookup the query in the GNS server.
   *
   * @param query
   * @param handler
   * @return A message with either a good response or an error.
   */
  public static Message lookupGnsServer(Message query, ClientRequestHandlerInterface handler) {
    // check for queries we can't handle
    int type = query.getQuestion().getType();
    // Was the query legitimate or implemented?
    if (!Type.isRR(type) && type != Type.ANY) {
      return errorMessage(query, Rcode.NOTIMP);
    }

    // extract the domain (guid) and field from the query
    final String fieldName = Type.string(query.getQuestion().getType());
    final Name requestedName = query.getQuestion().getName();
    final byte[] rawName = requestedName.toWire();
    final String domainName = querytoStringForGNS(rawName);

    NameResolution.getLogger().log(Level.FINE, "Trying GNS lookup for domain {0}", domainName);

    /* Create a response message and add records later */
    Message response = new Message(query.getHeader().getID());
    response.getHeader().setFlag(Flags.QR);
    if (query.getHeader().getFlag(Flags.RD)) {
      response.getHeader().setFlag(Flags.RA);
    }
    response.addRecord(query.getQuestion(), Section.QUESTION);
    response.getHeader().setFlag(Flags.AA);

    /* Request DNS fields of an alias and prepare a DNS response message */
    ArrayList<String> fieldNames = new ArrayList<>(Arrays.asList("A", "NS", "CNAME", "SOA", "PTR", "MX"));
    Boolean nameResolved = false;
    String nameToResolve = domainName;

    while (!nameResolved) {
      long resolveStart = System.currentTimeMillis();
      JSONObject fieldResponseJson = lookupGuidField(nameToResolve, null, fieldNames, handler);
      if (fieldResponseJson == null) {
        NameResolution.getLogger().log(Level.FINE, "GNS lookup for domain {0} failed.", domainName);
        return errorMessage(query, Rcode.NXDOMAIN);
      }
      /* Parse the response from GNS and create DNS records*/
      try {
        NameResolution.getLogger().log(Level.FINE, "fieldResponse all field:{0}", fieldResponseJson.toString());
        if (fieldResponseJson.has("A")) {
          String ip = fieldResponseJson.getString("A");
          ARecord gnsARecord = new ARecord(new Name(nameToResolve), DClass.IN, 60, InetAddress.getByName(ip));
          response.addRecord(gnsARecord, Section.ANSWER);
          nameResolved = true;
        }
        if (fieldResponseJson.has("NS")) {
          String ns = fieldResponseJson.getString("NS");
          NSRecord nsRecord = new NSRecord(new Name(nameToResolve), DClass.IN, 120, new Name(ns));
          response.addRecord(nsRecord, Section.AUTHORITY);

          /* Resolve NS Record name to an IP address and add it to ADDITIONAL section */
          JSONObject nsResponseJson = lookupGuidField(ns, fieldName, null, handler);
          //CommandResponse nsResponse = lookupGuidGnsServer(ns, fieldName, null, handler);
          if (nsResponseJson != null) {
            //if (nsResponse != null && !nsResponse.isError()) {
            String address = nsResponseJson.getString(ns);
            //String address = (new JSONArray(nsResponse.getReturnValue())).get(0).toString();
            NameResolution.getLogger().log(Level.FINE, "single field {0}", address);
            ARecord nsARecord = new ARecord(new Name(ns), DClass.IN, 60, InetAddress.getByName(address));
            response.addRecord(nsARecord, Section.ADDITIONAL);
          }
        }
        if (fieldResponseJson.has("MX")) {
          String mxname = fieldResponseJson.getString("MX");
          MXRecord mxRecord = new MXRecord(new Name(nameToResolve), DClass.IN, 120, 100, new Name(mxname));
          response.addRecord(mxRecord, Section.AUTHORITY);

          /* Resolve MX Record name to an IP address and add it to ADDITIONAL section */
          JSONObject mxResponseJson = lookupGuidField(mxname, fieldName, null, handler);
          //CommandResponse mxResponse = lookupGuidGnsServer(mxname, fieldName, null, handler);
          if (mxResponseJson != null) {
            //if (mxResponse != null && !mxResponse.isError()) {
            String address = mxResponseJson.getString(mxname);
            //String address = (new JSONArray(mxResponse.getReturnValue())).get(0).toString();
            NameResolution.getLogger().log(Level.FINER, "single field {0}", address);
            ARecord mxARecord = new ARecord(new Name(mxname), DClass.IN, 60, InetAddress.getByName(address));
            response.addRecord(mxARecord, Section.ADDITIONAL);
          }
        }
        if (fieldResponseJson.has("CNAME")) {
          /* Resolve CNAME alias to an IP address and add it to ADDITIONAL section */
          String cname = fieldResponseJson.getString("CNAME");
          CNAMERecord cnameRecord = new CNAMERecord(new Name(nameToResolve), DClass.IN, 60, new Name(cname));
          response.addRecord(cnameRecord, Section.ANSWER);
          nameToResolve = cname;
          continue;
        }
        DelayProfiler.updateDelay("ResolveName", resolveStart);
        if (!nameResolved) {
          /* We should reach here only if we fail to resolve to an IP address */
          NameResolution.getLogger().log(Level.FINER,
                  "Couldn''t resolve to an IP address for domain {0}", domainName);
          break;
        }
      } catch (JSONException e) {
        e.printStackTrace();
        return errorMessage(query, Rcode.NXDOMAIN);
      } catch (TextParseException | UnknownHostException e) {
        e.printStackTrace();
      }

    }
    NameResolution.getLogger().log(Level.FINER, "Outgoing response from GNS: {0}", response.toString());
    return response;
  }

  /**
   * Lookup the field or fields in the guid.
   * Returns a JSONObject containing the fields and values
   * or null if the domainName doesn't exist.
   *
   * @param domainName - the HRN of the guid
   * @param fieldName - the field to lookup (mutually exclusive with fieldNames)
   * @param fieldNames - the fields to lookup (mutually exclusive with fieldNames)
   * @param handler
   * @return a JSONObject containing the fields and values or null
   */
  public static JSONObject lookupGuidField(String domainName, String fieldName, ArrayList<String> fieldNames, ClientRequestHandlerInterface handler) {
    long startTime = System.currentTimeMillis();
    // Make an array of field names to fetch from fieldName or FieldNames
    String[] fieldArray = null;
    if (fieldName != null) {
      fieldArray = new String[1];
      fieldArray[0] = fieldName;
    } else if (fieldNames != null) {
      fieldArray = new String[fieldNames.size()];
      fieldArray = fieldNames.toArray(fieldArray);
    }
    // First we lookup the guid from the HRN
    GNSApp app = handler.getApp();
    NameRecord hrnNameRecord = null;
    try {
      hrnNameRecord = NameRecord.getNameRecordMultiUserFields(app.getDB(), domainName,
              ColumnFieldType.USER_JSON, HRN_GUID);
    } catch (RecordNotFoundException e) {
      // Normal result when the record doesn't exist
    } catch (FailedDBOperationException e) {
      NameResolution.getLogger().log(Level.SEVERE,
              "Problem getting guid for {0}: {1}", new Object[]{domainName, e});
    }
    DelayProfiler.updateDelay("lookupGuidField.guid", startTime);
    long fieldTime = System.currentTimeMillis();
    // If the HRS record isn't null we then get all the fields from the guid record
    NameRecord guidNameRecord = null;
    if (hrnNameRecord != null) {
      String guid = null;
      try {
        guid = hrnNameRecord.getValuesMap().getString(HRN_GUID);
      } catch (JSONException | FieldNotFoundException e) {
      }
      if (guid != null) {
        try {
          guidNameRecord = NameRecord.getNameRecordMultiUserFields(app.getDB(), guid,
                  ColumnFieldType.USER_JSON, fieldArray);
        } catch (RecordNotFoundException e) {
          // Normal result
        } catch (FailedDBOperationException e) {
          NameResolution.getLogger().log(Level.SEVERE,
                  "Problem getting guid for {0}: {1}", new Object[]{domainName, e});
        }
      }
    }
    DelayProfiler.updateDelay("lookupGuidField.field", fieldTime);
    DelayProfiler.updateDelay("lookupGuidField", startTime);
    // If the record actually exists we return all the values as a JSONObject
    if (guidNameRecord != null) {
      try {
        return guidNameRecord.getValuesMap();
      } catch (FieldNotFoundException e) {
        return null;
      }
    } else {
      return null;
    }
  }

  /**
   * Look up the local dns server cache.
   * Returns a {@link Message}.
   *
   * @param query
   * @param dnsCache
   * @return a Message
   */
  public static Message lookupDnsCache(Message query, Cache dnsCache) {
    // check for queries we can't handle
    int type = query.getQuestion().getType();
    // Was the query legitimate or implemented?
    if (!Type.isRR(type) && type != Type.ANY) {
      return errorMessage(query, Rcode.NOTIMP);
    }
    // extract the domain (guid) and field from the query
    final String fieldName = Type.string(query.getQuestion().getType());
    final Name requestedName = query.getQuestion().getName();
    final byte[] rawName = requestedName.toWire();
    final String lookupName = querytoStringForGNS(rawName);

    NameResolution.getLogger().log(Level.FINER, "Looking up name in cache: {0}", lookupName);
    SetResponse lookupresult = dnsCache.lookupRecords(requestedName, Type.ANY, Credibility.NORMAL);
    if (lookupresult.isSuccessful()) {
      Message response = new Message(query.getHeader().getID());
      response.getHeader().setFlag(Flags.QR);
      if (query.getHeader().getFlag(Flags.RD)) {
        response.getHeader().setFlag(Flags.RA);
      }
      response.addRecord(query.getQuestion(), Section.QUESTION);
      response.getHeader().setFlag(Flags.AA);
      ArrayList<Name> cnameNames = new ArrayList<>();
      // Write the response
      for (RRset rrset : lookupresult.answers()) {
        NameResolution.getLogger().log(Level.FINE, "{0}\n", rrset.toString());
        Iterator<?> rrItr = rrset.rrs();
        while (rrItr.hasNext()) {
          Record curRecord = (Record) rrItr.next();
          response.addRecord(curRecord, Section.ANSWER);
          if (curRecord.getType() == Type.CNAME) {
            cnameNames.add(((CNAMERecord) curRecord).getAlias());
          }
        }
      }
      if (cnameNames.isEmpty()) {
        return response;
      }
      // For all CNAMES in the response, add their A records
      for (Name cname : cnameNames) {
        NameResolution.getLogger().log(Level.FINE, 
                "Looking up CNAME in cache: {0}", cname.toString());
        SetResponse lookUpResult = dnsCache.lookupRecords(cname, Type.ANY, Credibility.NORMAL);
        if (lookUpResult.isSuccessful()) {
          for (RRset rrset : lookUpResult.answers()) {
            NameResolution.getLogger().log(Level.FINE, "{0}\n", rrset.toString());
            Iterator<?> rrItr = rrset.rrs();
            while (rrItr.hasNext()) {
              Record curRecord = (Record) rrItr.next();
              response.addRecord(curRecord, Section.ANSWER);
            }
          }
        }
      }
      return response;
    } else {
      return errorMessage(query, Rcode.NOTIMP);
    }
  }

  /**
   * Returns a {@link Message} with an error in it if the query is not good.
   *
   * @param query
   * @return a Message
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
   * @return true if the response is OK
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
   *
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
    if (question != null) {
      response.addRecord(question, Section.QUESTION);
    }
    response.getHeader().setRcode(rcode);
    return response;
  }

  /**
   * Builds a pretty string showing the query and response.
   * Used for debugging purposes.
   *
   * @param query
   * @param response
   * @return a string
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

  /**
   * Creates a string out of raw bytes[] from qname section
   * of the DNS query message. This routine, when used, adds
   * '.' at the end for using it for GNS lookups.
   *
   * @param qname
   * @return questionString
   */
  public static String querytoStringForGNS(byte[] qname) {
    return querytoStringForGNS(qname, false);
  }

  /**
   * Creates a string out of raw bytes[] from qname section
   * of the DNS query message.
   *
   * @param qname
   * @param omitFinalDot
   * @return questionString
   */
  public static String querytoStringForGNS(byte[] qname, boolean omitFinalDot) {
    int curPos = 0;
    int labelSize = 0;
    int totalSize = qname.length - 1;
    byte[] subName;
    StringBuilder sb = new StringBuilder();
    while (curPos < totalSize) {
      labelSize = (int) qname[curPos];
      if (labelSize == 0) {
        break;
      }
      subName = Arrays.copyOfRange(qname, curPos + 1, curPos + labelSize + 1);
      sb.append(new String(subName));
      curPos += (labelSize + 1);
      if (curPos == totalSize) {
        if (!omitFinalDot) {
          sb.append(".");
        }
        break;
      }
      sb.append(".");
    }
    return sb.toString();
  }
}
