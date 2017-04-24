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

import static edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess.HRN_GUID;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

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

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

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
   * @param addr 
   * @param query
   * @param handler
   * @return A message with either a good response or an error.
   */
  public static Message lookupGnsServer(InetAddress addr, Message query, ClientRequestHandlerInterface handler) {
    // check for queries we can't handle
    int type = query.getQuestion().getType();
    // Was the query legitimate or implemented?
    if (!Type.isRR(type) && type != Type.ANY) {
      return errorMessage(query, Rcode.NOTIMP);
    }

    // extract the domain (guid) and field from the query
    final int fieldName = query.getQuestion().getType();
    final Name requestedName = query.getQuestion().getName();
    final byte[] rawName = requestedName.toWire();
    final String domainName = querytoStringForGNS(rawName);
    // The domain name must be an absolute name, i.e., ended with a dot
    assert(domainName.endsWith(".")):"The domain name "+domainName+"to resolve is not an absolute name!";
    
    /**
     *  The query type or domain name can't be null, otherwise return an error message
     */
    if(Type.string(fieldName) ==null || domainName==null){
    	return errorMessage(query, Rcode.NXDOMAIN);
    }
    NameResolution.getLogger().log(Level.FINE, "Trying GNS lookup for domain {0}, type {1}", new Object[]{domainName, Type.string(fieldName)});
    
    /**
     *  Create a response message, build the header first.
     *  The response is constructed later after GNS query.
     */
    Message response = new Message(query.getHeader().getID());
    response.getHeader().setFlag(Flags.QR);
    if (query.getHeader().getFlag(Flags.RD)) {
      response.getHeader().setFlag(Flags.RA);
    }
    response.addRecord(query.getQuestion(), Section.QUESTION);
    response.getHeader().setFlag(Flags.AA);

    /**
     * Request DNS fields of an alias and prepare a DNS response message 
     */
    ArrayList<String> fields = new ArrayList<>(Arrays.asList("A", "NS", "CNAME", "SOA", "PTR", "MX"));
    
    /**
     * <p>
     * RFC 1034: the additional section "carries RRs(Resource Records) which may be helpful in
     * 			using the RRs in the other section"
     * RFC 2181: data you put in the additional section can never be promoted into real answers.
     * 
     * <p>When a DNS client needs to look up a name used in a program, it queries DNS servers to resolve the name. 
     * Each query message the client sends contains three pieces of information, specifying a question for the server to answer:
     * 1. A specified DNS domain name, stated as a fully qualified domain name (FQDN).
     * 2. A specified query type, which can either specify a resource record (RR) by type or a specialized type of query operation.
     * 3. A specified class for the DNS domain name. For DNS servers running the Windows operating system, this should always be specified as the Internet (IN) class.
     * 
     * <p>The information is retrieved from GNS based on the queried domain.
     * <p>The response is constructed based on the query type,
     * 1. A: return A records in ANSWER section, NS records in AUTHORITY section, A records of name servers in ADDITIONAL section
     * 2. NS: return NS records in ANSWER section, A records of name servers in ADDITIONAL section
     * 3. MX: return MX records in ANSWER section, NS records in AUTHORITY section, A record of name servers in ADDITIONAL section
     * 4. CNAME: return CNAME records in in ANSWER section, NS records in AUTHORITY section, A record of name servers in ADDITIONAL section
     * 
     * Records in ADDITIONAL section is not required, we do a best-effort resolution for the names in ADDITIONAL section.
     */
	  long resolveStart = System.currentTimeMillis();      
	  
	  JSONObject fieldResponseJson = lookupGuidField(addr.getHostAddress().toString(), query.getHeader().getID(), domainName, null, fields, handler);
	  if (fieldResponseJson == null) {
	    NameResolution.getLogger().log(Level.FINE, "GNS lookup for domain {0} failed.", domainName);
	    return errorMessage(query, Rcode.NXDOMAIN);
	  }
	  NameResolution.getLogger().log(Level.FINE, "fieldResponse all fields (NS, MX, CNAME, A): {0}", fieldResponseJson.toString());
	  
      switch(fieldName){
      case Type.NS:
	      {
	    	  JSONObject obj = getNSRecordsFromNSField(fieldResponseJson, domainName);
	    	  if(obj != null){
	    		 try {
					JSONArray nsList = obj.getJSONArray("NS");
					JSONArray aList = obj.getJSONArray("A");
					for (int i=0; i<nsList.length(); i++){
						response.addRecord((Record) nsList.get(i), Section.ANSWER);
					}
					for (int i=0; i<aList.length(); i++){
						response.addRecord((Record) aList.get(i), Section.ADDITIONAL);
					}
				} catch (JSONException e) {
					// do nothing, this happens only because some record is corrupted
				}	    		  
	    	  } else {
	    		// I don't have the requested A record, you must ask a wrong guy
	    		return errorMessage(query, Rcode.NXDOMAIN);
	    	  }
	      }
		  break;
      case Type.A:
	      {
	    	  // Get A records from retrieved GNS record
	    	  JSONArray aList = getARecordsFromAField(fieldResponseJson, domainName);
	    	  if(aList != null){
		    	  for (int i=0; i<aList.length(); i++){
		    		  try {
						response.addRecord((Record) aList.get(i), Section.ANSWER);
					} catch (JSONException e) {
						// trash the record
					}
		    	  }
	    	  } else{
	    		  // I don't have the requested A record, you must ask a wrong guy
	    		  return errorMessage(query, Rcode.NXDOMAIN);
	    	  }
	    	  //Get NS record if we can
	    	  JSONObject obj = getNSRecordsFromNSField(fieldResponseJson, domainName);
	    	  if(obj != null){
	    		 try {
					JSONArray nsList = obj.getJSONArray("NS");
					JSONArray aNSList = obj.getJSONArray("A");
					for (int i=0; i<nsList.length(); i++){
						response.addRecord((Record) nsList.get(i), Section.AUTHORITY);
					}
					for (int i=0; i<aNSList.length(); i++){
						response.addRecord((Record) aNSList.get(i), Section.ADDITIONAL);
					}
				} catch (JSONException e) {
					// do nothing, this happens only because some record is corrupted
				}	    		  
	    	  }
	      }
	      break;
      case Type.MX:
	      {
	    	  JSONObject obj = getMXRecordsFromMXField(fieldResponseJson, domainName);
	    	  NameResolution.getLogger().log(Level.FINE, "MX record for domain {0} is {1}", new Object[]{domainName, obj});
	    	  if(obj != null){
	    		  try{
		    	  JSONArray mxList = obj.getJSONArray("MX");
		    	  JSONArray aList = obj.getJSONArray("A");
		    	  for (int i=0; i<mxList.length(); i++){
		    		  response.addRecord((Record) mxList.get(i), Section.ANSWER);
		    	  }
		    	  for (int i=0; i<aList.length(); i++) {
		    		  response.addRecord((Record) aList.get(i), Section.ADDITIONAL);
		    	  }
	    		  } catch (JSONException e) {
	    			// do nothing, this happens only because some record is corrupted
	    		  }
	    	  } else{
	    		// I don't have the requested MX record, you must ask a wrong guy
	    		  return errorMessage(query, Rcode.NXDOMAIN);
	    	  }
	    	  //Get NS record if we can
	    	  obj = getNSRecordsFromNSField(fieldResponseJson, domainName);
	    	  if(obj != null){
	    		 try {
					JSONArray nsList = obj.getJSONArray("NS");
					JSONArray aNSList = obj.getJSONArray("A");
					for (int i=0; i<nsList.length(); i++){
						response.addRecord((Record) nsList.get(i), Section.AUTHORITY);
					}
					for (int i=0; i<aNSList.length(); i++){
						response.addRecord((Record) aNSList.get(i), Section.ADDITIONAL);
					}
				} catch (JSONException e) {
					// do nothing, this happens only because some record is corrupted
				}	    		  
	    	  }
	      }
    	  break;
      case Type.CNAME:
	      {
	    	  if (fieldResponseJson.has("CNAME")) {
	    		// get CNAME alias, no need to resolve it to an IP address
				try {
					String cname = fieldResponseJson.getString("CNAME");
					// The cname must be an absolute name, i.e., ended with a dot
					if (!cname.endsWith(".")){
						cname = cname +".";
					}
					CNAMERecord cnameRecord = new CNAMERecord(new Name(domainName), DClass.IN, 60, new Name(cname));
					response.addRecord(cnameRecord, Section.ANSWER);
				} catch (JSONException | TextParseException e) {
					
				}
	              
	    	  } else {
	    		// I don't have the requested CNAME record, you must ask a wrong guy
	    		 return errorMessage(query, Rcode.NXDOMAIN);
	    	  }
	      }
    	  break;
	  default:
		  // we haven't implemented yet
		  return errorMessage(query, Rcode.NOTIMPL);
      }
      
      
    DelayProfiler.updateDelay("ResolveName", resolveStart);
    NameResolution.getLogger().log(Level.FINER, "Outgoing response from GNS: {0}", response.toString());
    return response;
  }

  /**
   * retrieve all A records from A field of a JSON object
   * 
   * @return
   */
  private static JSONArray getARecordsFromAField(JSONObject fieldResponseJson, String nameToResolve){
	  JSONArray aList = new JSONArray();	  
      /**
       * Format of A record in GNS:
       * {
       * 	"A":
       * 		{
       * 			"record": String[ip1, ip2, ...],
       * 			"ttl": int
       * 		}
       * }
       */
      if (fieldResponseJson.has("A")) {
    	  JSONArray records = null;
    	  int ttl = 60;
    	  try{
    		  JSONObject recordObj = fieldResponseJson.getJSONObject("A");
	    	  records = recordObj.getJSONArray(ManagedDNSServiceProxy.RECORD_FIELD);
	    	  ttl = recordObj.getInt(ManagedDNSServiceProxy.TTL_FIELD);
    	  }catch(JSONException e){
    		  // something is wrong with the JSON object, return null
    		  e.printStackTrace();
    		  return null;
    	  }
    	  // The records may contain multiple ip addresses
    	  for(int i=0; i<records.length(); i++){
			try {
				String ip = records.getString(i);
				ARecord gnsARecord = new ARecord(new Name(nameToResolve), DClass.IN, ttl, InetAddress.getByName(ip));
				aList.put(gnsARecord);
			} catch (JSONException | TextParseException | UnknownHostException e) {
				// do nothing, just trash this record
				e.printStackTrace();
			}  		  
    	  }
      } else {
    	  // there is no A record in the original GNS record, so return null
    	  return null;
      }	  
	  return aList;
  }
  
  /**
   * retrieve all NS records and the corresponding A records from NS field of a JSON object.
   * the key "NS" contains a list of all NS records
   * the key "A" contains a list of all A records, which must be put into ADDITIONAL section
   * 
   */
  private static JSONObject getNSRecordsFromNSField(JSONObject fieldResponseJson, String nameToResolve){
	  JSONObject obj = new JSONObject();
	  JSONArray aList = new JSONArray();
	  JSONArray nsList = new JSONArray();
	  
	  /**
       * Format of NS record in GNS:
       * {
       * 	"NS":
       * 		{
       * 			"record":[(ns1, addr1), (ns2, addr2), ...],
       * 			"ttl":int
       * 		}
       * }
       * 
       */
      if (fieldResponseJson.has("NS")) {
    	  JSONArray records = null;
    	  int ttl = 3600;
    	  try{
	    	  JSONObject recordObj = fieldResponseJson.getJSONObject("NS");
	    	  records = recordObj.getJSONArray(ManagedDNSServiceProxy.RECORD_FIELD);
	    	  ttl = recordObj.getInt(ManagedDNSServiceProxy.TTL_FIELD);
    	  } catch (JSONException e){
    		  // something is wrong with the JSON object, return null
    		  e.printStackTrace();
    		  return null;
    	  }
    	  // The records may contain multiple NS records
    	  for(int i=0; i<records.length(); i++){
			  try{
	    		  JSONArray record = records.getJSONArray(i);
	    		  String ns = record.getString(0);
	    		  // It must be an absolute name, i.e., the string must be ended  with a dot, e.g., example.com.
	    		  if(!ns.endsWith(".")){
	    			  ns = ns+".";
	    		  }
	    		  NSRecord nsRecord = new NSRecord(new Name(nameToResolve), DClass.IN, ttl, new Name(ns));
	    		  nsList.put(nsRecord);
	    		  // address can be null as the domain name might use other service as its name server
	          	  if(record.length()==2){
	          		  String address = record.getString(1);
	          		  ARecord nsARecord = new ARecord(new Name(ns), DClass.IN, ttl, InetAddress.getByName(address));
	          		  aList.put(nsARecord);
	          	  } else {
	          		  // no IP address in the record for the name server
	          	  }
			  } catch (JSONException | TextParseException | UnknownHostException e) {
				  // do nothing and trash this record
				  e.printStackTrace();
			  }
		  } 
      } else{
    	  // No NS record, return null
      }
      
	  try {
		obj.put("NS", nsList);
		obj.put("A", aList);
	  } catch (JSONException e) {
			// return a null if JSON operation fails
			return null;
	  }
	  return obj;
  }
  
  /**
   * retrieve MX record from MX field of a JSON object
   * the key "MX" contains a list of all MX records
   * the key "A" contains a list of all A records, which must be put into ADDITIONAL section
   * 
   * @return
   */
  private static JSONObject getMXRecordsFromMXField(JSONObject fieldResponseJson, String nameToResolve){
	  JSONObject obj = new JSONObject();
	  JSONArray mxList = new JSONArray();
	  JSONArray aList = new JSONArray();
	  
	  if (fieldResponseJson.has("MX")) {
		  JSONArray records = null;
		  int ttl = 3600;
		  try{
	          JSONObject mxname = fieldResponseJson.getJSONObject("MX");
	          records = mxname.getJSONArray(ManagedDNSServiceProxy.RECORD_FIELD);
	          ttl = mxname.getInt(ManagedDNSServiceProxy.TTL_FIELD);
		  } catch (JSONException e){
			  // something is wrong with the JSON object, return null
			  e.printStackTrace();
			  return null;
		  }
		  NameResolution.getLogger().log(Level.FINE, "Get MX records list: {0}", records.toString());
          // The records may contain multiple NS records
          for(int i=0; i<records.length(); i++){
        	  try{
	        	  JSONArray record = records.getJSONArray(i);
	        	  String pString = record.getString(0);
	        	  int priority = Integer.parseInt(pString);
	        	  String host = record.getString(1);	
	        	  // the host must be an absolute name, i.e., ended with a dot
	        	  if(!host.endsWith(".")){
	        		  host = host + ".";
	        	  }
	        	  MXRecord mxRecord = new MXRecord(new Name(nameToResolve), DClass.IN, ttl, priority, new Name(host));
	        	  mxList.put(mxRecord);
	        	  
	        	  if(record.length() == 3){
	        		  String address = record.getString(2);
		    		  ARecord mxARecord = new ARecord(new Name(host), DClass.IN, ttl, InetAddress.getByName(address));
		    		  aList.put(mxARecord);
	        	  } else {
	        		  // no IP address in the record for the mail server
	        	  }
        	  } catch (JSONException | TextParseException | UnknownHostException e) {
        		  // trash this record
        		  e.printStackTrace();
        	  }
          }    
        }
	  
	  try {
		  obj.put("MX", mxList);
		  obj.put("A", aList);
	  } catch (JSONException e) {
		  // return a null if JSON operation fails
		  return null;
	  }
	  return obj;
  }
  
  
  /**
   * Lookup the field or fields in the guid.
   * Returns a JSONObject containing the fields and values
   * or null if the domainName doesn't exist.
   * @param addr 
   * @param id 
   * @param domain - the HRN of the guid
   * @param field - the field to lookup (mutually exclusive with fieldNames)
   * @param fields - the fields to lookup (mutually exclusive with fieldNames)
   * @param handler
   * @return a JSONObject containing the fields and values or null
   */
  public static JSONObject lookupGuidField(String addr, int id, String domain, String field, ArrayList<String> fields, ClientRequestHandlerInterface handler) {
    /**
     * Querying multiple types together is allowed in DNS protocol, but practically not supported.
     * Therefore, no need for us to implement support for multi-type query.
     */
    
    /**
     * 1. Lookup guid for the domain name
     */
    String guid = null;
    final ValuesMap result;
    try{
	    result = NSFieldAccess.lookupJSONFieldLocalNoAuth(null, domain,
	            HRN_GUID, handler.getApp(), false);
	    if (result != null) {
	        guid = result.getString(HRN_GUID);
	    }
    } catch (FailedDBOperationException | JSONException e) {
    	NameResolution.getLogger().log(Level.FINE,
                "No guid for {0}: {1}", new Object[]{domain, e});
    	return null;
    }
    
    /**
     * 2. Lookup the record
     */
    JSONObject value = null;
    if(guid != null){
    	// Generate a DNS header for local read 
    	InternalRequestHeader header = new InternalRequestHeader(){

			@Override
			public long getOriginatingRequestID() {
				return id;
			}

			@Override
			public String getOriginatingGUID() {
				try {
					return result.getString(HRN_GUID);
				} catch (JSONException e) {
					return null;
				}
			}

			@Override
			public int getTTL() {
				return InternalRequestHeader.DEFAULT_TTL;
			}

			@Override
			public boolean hasBeenCoordinatedOnce() {
				// DNS request does not need coordination
				return false;
			}
			
			@Override
			public String getSourceAddress(){
				return addr;
			}
    		
    	};
    	try {
			value = NSFieldAccess.lookupFieldsLocalNoAuth(header, guid, fields, ColumnFieldType.USER_JSON, handler);
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
