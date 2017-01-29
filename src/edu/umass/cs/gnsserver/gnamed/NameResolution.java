
package edu.umass.cs.gnsserver.gnamed;

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.xbill.DNS.ARecord;
import org.xbill.DNS.CNAMERecord;
import org.xbill.DNS.Cache;
import org.xbill.DNS.Credibility;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Flags;
import org.xbill.DNS.Header;
import org.xbill.DNS.MXRecord;
import org.xbill.DNS.Message;
import org.xbill.DNS.NSRecord;
import org.xbill.DNS.Name;
import org.xbill.DNS.Opcode;
import org.xbill.DNS.RRset;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.Record;
import org.xbill.DNS.Section;
import org.xbill.DNS.SetResponse;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess.HRN_GUID;


public class NameResolution {

  private static final Logger LOG = Logger.getLogger(NameResolution.class.getName());


  public static final Logger getLogger() {
    return LOG;
  }


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


    Message response = new Message(query.getHeader().getID());
    response.getHeader().setFlag(Flags.QR);
    if (query.getHeader().getFlag(Flags.RD)) {
      response.getHeader().setFlag(Flags.RA);
    }
    response.addRecord(query.getQuestion(), Section.QUESTION);
    response.getHeader().setFlag(Flags.AA);


    ArrayList<String> fields = new ArrayList<>(Arrays.asList("A", "NS", "CNAME", "SOA", "PTR", "MX"));
    Boolean nameResolved = false;
    String nameToResolve = domainName;

    while (!nameResolved) {
      long resolveStart = System.currentTimeMillis();
      JSONObject fieldResponseJson = lookupGuidField(nameToResolve, null, fields, handler);
      if (fieldResponseJson == null) {
        NameResolution.getLogger().log(Level.FINE, "GNS lookup for domain {0} failed.", domainName);
        return errorMessage(query, Rcode.NXDOMAIN);
      }
      // Parse the response from GNS and create DNS records 
      try {
        NameResolution.getLogger().log(Level.FINE, "fieldResponse all field:{0}", fieldResponseJson.toString());
        if (fieldResponseJson.has("A")) {
          JSONObject recordObj = fieldResponseJson.getJSONObject("A");
          JSONArray records = recordObj.getJSONArray(ManagedDNSServiceProxy.RECORD_FIELD);
          int ttl = recordObj.getInt(ManagedDNSServiceProxy.TTL_FIELD);
          // The records may contain multiple ip addresses
          for(int i=0; i<records.length(); i++){
      		String ip = records.getString(i);
	        ARecord gnsARecord = new ARecord(new Name(nameToResolve), DClass.IN, ttl, InetAddress.getByName(ip));
	        response.addRecord(gnsARecord, Section.ANSWER);
      	  }
          nameResolved = true;

        }
        if (fieldResponseJson.has("NS")) {
          JSONObject recordObj = fieldResponseJson.getJSONObject("NS");	
          JSONArray records = recordObj.getJSONArray(ManagedDNSServiceProxy.RECORD_FIELD);
          int ttl = recordObj.getInt(ManagedDNSServiceProxy.TTL_FIELD);
          // The records may contain multiple ip addresses
          for(int i=0; i<records.length(); i++){
        	  JSONArray record = records.getJSONArray(i);
        	  String ns = record.getString(0);
        	  String address = record.getString(1);
        	  NSRecord nsRecord = new NSRecord(new Name(nameToResolve), DClass.IN, ttl, new Name(ns));
        	  response.addRecord(nsRecord, Section.AUTHORITY);
        	  ARecord nsARecord = new ARecord(new Name(ns), DClass.IN, 60, InetAddress.getByName(address));
        	  response.addRecord(nsARecord, Section.ADDITIONAL);
          }


        }
        if (fieldResponseJson.has("MX")) {
          String mxname = fieldResponseJson.getString("MX");
          MXRecord mxRecord = new MXRecord(new Name(nameToResolve), DClass.IN, 120, 100, new Name(mxname));
          response.addRecord(mxRecord, Section.AUTHORITY);

          // Resolve MX Record name to an IP address and add it to ADDITIONAL section 
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
          // Resolve CNAME alias to an IP address and add it to ADDITIONAL section 
          String cname = fieldResponseJson.getString("CNAME");
          CNAMERecord cnameRecord = new CNAMERecord(new Name(nameToResolve), DClass.IN, 60, new Name(cname));
          response.addRecord(cnameRecord, Section.ANSWER);
          nameToResolve = cname;
          continue;
        }
        DelayProfiler.updateDelay("ResolveName", resolveStart);
        if (!nameResolved) {
          // We should reach here only if we fail to resolve to an IP address
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


  public static JSONObject lookupGuidField(String domain, String field, ArrayList<String> fields, ClientRequestHandlerInterface handler) {

    

    String guid = null;
    try{
	    ValuesMap result = NSFieldAccess.lookupJSONFieldLocalNoAuth(null, domain,
	            HRN_GUID, handler.getApp(), false);
	    if (result != null) {
	        guid = result.getString(HRN_GUID);
	    }
    } catch (FailedDBOperationException | JSONException e) {
    	NameResolution.getLogger().log(Level.FINE,
                "No guid for {0}: {1}", new Object[]{domain, e});
    }
    

    JSONObject value = null;
    if(guid != null){
    	//FIXME: the internal request header should not be null
    	try {
			value = NSFieldAccess.lookupFieldsLocalNoAuth(null, guid, fields, ColumnFieldType.USER_JSON, handler);
		} catch (FailedDBOperationException e) {
			NameResolution.getLogger().log(Level.FINE,
	                "Fetching record failed for {0}: {1}", new Object[]{domain, e});
		}
    }else{
    	NameResolution.getLogger().log(Level.FINE,
                "No guid for {0} is found", new Object[]{domain});
    }
    return value;
    

  }


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


  public static Message formErrorMessage(byte[] in) {
    Header header;
    try {
      header = new Header(in);
    } catch (IOException e) {
      return null;
    }
    return buildErrorMessage(header, Rcode.FORMERR, null);
  }


  public static Message errorMessage(Message query, int rcode) {
    return buildErrorMessage(query.getHeader(), rcode, query.getQuestion());
  }


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


  public static String querytoStringForGNS(byte[] qname) {
    return querytoStringForGNS(qname, false);
  }


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
