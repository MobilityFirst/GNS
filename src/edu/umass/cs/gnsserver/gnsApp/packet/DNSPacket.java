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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.packet;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.nio.interfaces.Stringifiable;

import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.Set;

/**
 **
 * Packet transmitted between the local name server and a name server. All communications inside of the domain protocol are carried
 * in a single DNS packet. The packet contains the query from a local name server and a response from the name server.
 *
 *
 * @param <NodeIDType>
 */
public class DNSPacket<NodeIDType> extends BasicPacketWithSignatureInfoAndCCPAddress implements Request {

  private final static String HEADER = "dns_header";
  private final static String GUID = "dns_guid";
  private final static String KEY = "dns_key";
  private final static String KEYS = "dns_keys";
  private final static String TIME_TO_LIVE = "ttlAddress";
  private final static String RECORD_VALUE = "recordValue";
  private final static String SOURCE_ID = "sourceId";
  private final static String RESPONDER = "rspndr";
  private final static String RETURN_FORMAT = "format";
  // instrumentation
  //private final static String LOOKUP_TIME = "lookupTime";
  /*
   * The header, guid, key and lnsId are called the Question section because
   * they are all that is necessary for a query.
   */
  /**
   * Packet header *
   */
  private Header header;
  /**
   * Name in the query *
   */
  private final String guid;
  /**
   * The key of the value key pair. Mutually exclusive with keys.
   */
  private final String key;
  /**
   * The keys when we are doing a multiple field query lookup. Mutually exclusive with key.
   */
  private final List<String> keys;
  /**
   * This is the id of the source of the request, null means the client is the intercessor
   * of the LNS handling the request. Otherwise it will be the ID of a NameServer.
   */
  private NodeIDType sourceId;
  /**
   * Time interval (in seconds) that the resource record may be cached before it should be discarded
   */
  /*
   * The ttl, recordValue, and activeNameServers are called the Response section because
   * they are the values that one gets in response to a query.
   */
  private int ttl = GNS.DEFAULT_TTL_SECONDS;
  /**
   * The value that is getting sent back to the client. MOST OF THE TIME THIS WILL HAVE JUST A SINGLE KEY/VALUE, but sometimes we
   * return the entire record. When it's a single key/value the key will be the same as the qrecordKey.
   */
  private ValuesMap recordValue;
  /**
   * Determines the format of the return value that we toString back in the response packet.
   */
  private final ColumnFieldType returnFormat;
  // instrumentation
  /**
   * For response packets this is the node that responded
   */
  private NodeIDType responder = null;
//  /**
//   * Database lookup time instrumentation
//   */
//  private int lookupTime = -1;

  /**
   * Constructs a packet for querying a name server for name information.
   *
   * @param sourceId - the id of the server sending the request, equal to LOCAL_SOURCE_ID means the intercessor of the LNS (the usual case)
   * @param id
   * @param guid
   * @param key
   * @param keys
   * @param accessor
   * @param signature
   * @param message
   * @param returnFormat
   */
  public DNSPacket(NodeIDType sourceId, int id, String guid, String key, List<String> keys,
          ColumnFieldType returnFormat,
          String accessor, String signature, String message) {
    super(null, accessor, signature, message); // lnsAddress is null
    this.type = Packet.PacketType.DNS;
    this.header = new Header(id, DNSRecordType.QUERY, NSResponseCode.NO_ERROR);
    this.guid = guid;
    this.key = key;
    this.keys = keys;
    this.sourceId = sourceId != null ? sourceId : null;
    this.responder = null;
    this.returnFormat = returnFormat;
  }

  /**
   * Creates a DNS packet for when you just have a single field you want to return.
   * This packet has both a query and response section.
   *
   * @param sourceId
   * @param id
   * @param name
   * @param key
   * @param fieldValue
   * @param TTL
   * @param activeNameServers
   */
  public DNSPacket(NodeIDType sourceId, int id, String name, String key, ResultValue fieldValue, int TTL, Set<Integer> activeNameServers) {
    this(sourceId, id, name, key, null, new ValuesMap(), TTL, activeNameServers);
    // slide that baby in...
    this.recordValue.putAsArray(key, fieldValue);
  }

  /**
   * Creates a DNS packet for when you want to return multiple fields from a NameRecord.
   * This packet has both a query and response section.
   *
   * @param sourceId
   * @param id
   * @param name
   * @param key
   * @param keys
   * @param entireRecord
   * @param TTL
   * @param activeNameServers
   */
  public DNSPacket(NodeIDType sourceId, int id, String name, String key, List<String> keys, ValuesMap entireRecord, int TTL, Set<Integer> activeNameServers) {
    super(null); // lnsAddress is null and no sigs for this baby
    this.type = Packet.PacketType.DNS;
    this.header = new Header(id, DNSRecordType.RESPONSE, NSResponseCode.NO_ERROR);
    this.guid = name;
    this.key = key;
    this.keys = keys;
    this.sourceId = sourceId;
    this.recordValue = entireRecord;
    this.ttl = TTL;
    this.responder = null;
    this.returnFormat = null;
  }

  /**
   **
   * Constructs a packet from a JSONObject that represents a DNS packet.
   * This packet has both a query and response section.
   * The response section will be empty if this is an error response.
   *
   * @param json JSONObject that represents a DNS packet
   * @param unstringer
   * @throws org.json.JSONException
   */
  public DNSPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json.optString(CCP_ADDRESS, null), json.optInt(CCP_PORT, INVALID_PORT),
            json.optString(ACCESSOR, null), json.optString(SIGNATURE, null), json.optString(MESSAGE, null));
    this.type = Packet.getPacketType(json);
    this.header = new Header(json.getJSONObject(HEADER));
    this.guid = json.getString(GUID);
    if (json.has(KEY)) {
      this.key = json.getString(KEY);
    } else {
      this.key = null;
    }
    if (json.has(KEYS)) {
      this.keys = JSONUtils.JSONArrayToArrayListString(json.getJSONArray(KEYS));
    } else {
      this.keys = null;
    }
    this.sourceId = json.has(SOURCE_ID) ? unstringer.valueOf(json.getString(SOURCE_ID)) : null;
    // read the optional responder if it is there
    this.responder = json.has(RESPONDER) ? unstringer.valueOf(json.getString(RESPONDER)) : null;
    this.returnFormat = json.has(RETURN_FORMAT) ? ColumnFieldType.valueOf(json.getString(RETURN_FORMAT)) : null;

    // These will only be present in non-error response packets
    if (header.isResponse() && !header.isAnyKindOfError()) {
      this.ttl = json.getInt(TIME_TO_LIVE);
      if (json.has(RECORD_VALUE)) {
        this.recordValue = new ValuesMap(json.getJSONObject(RECORD_VALUE));
      }
    }
    //this.lookupTime = json.optInt(LOOKUP_TIME, -1);
  }

  /**
   **
   * Converts this packet's query section to a JSONObject.
   * In other words, this packet has just query section and
   * doesn't contain the ttl, activeNameServers or recordValue.
   * But it can have an error value because that value is in the header.
   *
   * @return JSONObject that represents a DNS packet's query section
   * @throws org.json.JSONException
   */
  public JSONObject toJSONObjectQuestion() throws JSONException {
    JSONObject json = new JSONObject();
    // include signatures and but not response section
    addToJSONObjectHelper(json, true, false);
    return json;
  }

  /**
   * Converts this packet's query section to a JSONObject and removes all the
   * signature information as well.
   *
   * @return a JSONObject
   * @throws org.json.JSONException
   */
  public JSONObject toJSONObjectForErrorResponse() throws JSONException {
    JSONObject json = new JSONObject();
    // no signatures and no response section
    addToJSONObjectHelper(json, false, false);
    return json;
  }

  /**
   **
   * Converts this packet's response section to a JSONObject
   *
   * @return JSONObject that represents a DNS packet's response section
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    addToJSONObject(json);
    return json;
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    addToJSONObjectHelper(json, true, true);
  }

  // conditionally writes various parts depending on whether we are a query or a response
  private void addToJSONObjectHelper(JSONObject json, boolean includeSignatureSection, boolean includeReponseSection) throws JSONException {
    super.addToJSONObject(json, includeSignatureSection);
    Packet.putPacketType(json, getType());
    // query section
    json.put(HEADER, getHeader().toJSONObject());
    if (key != null) {
      json.put(KEY, key);
    }
    if (keys != null) {
      json.put(KEYS, keys);
    }
    json.put(GUID, guid);
    if (sourceId != null) {
      json.put(SOURCE_ID, sourceId);
    }
    if (returnFormat != null) {
      json.put(RETURN_FORMAT, returnFormat.name());
    }
    // this goes in with query (if it's not empty) in case it's an error response and we want to know the reponder
    if (responder != null) {
      json.put(RESPONDER, responder.toString());
    }
    // response section
    if (includeReponseSection) {
      json.put(TIME_TO_LIVE, getTTL());
      if (recordValue != null) {
        json.put(RECORD_VALUE, recordValue);
      }
//      if (lookupTime != -1) {
//        json.put(LOOKUP_TIME, lookupTime);
//      }
    }
  }

  /**
   *
   * Returns true if the packet is a query, false otherwise
   *
   * @return true if the packet is a query
   */
  public boolean isQuery() {
    return getHeader().isQuery();
  }

  /**
   * Returns true if the packet is a response, false otherwise
   *
   * @return true if the packet is a response
   */
  public boolean isResponse() {
    return getHeader().isResponse();
  }

  /**
   **
   * Returns true if the packet contains a response error, false otherwise
   *
   * @return true if the packet contains a response error
   */
  public boolean containsAnyError() {
    return getHeader().isAnyKindOfError();
  }

  /**
   * Returns true if the packet contains an InvalidActiveNSError.
   * 
   * @return true or false
   */
  public boolean containsInvalidActiveNSError() {
    return getHeader().isInvalidActiveNSError();
  }

  /**
   * Returns the ID for this query from the packet header. Used by the requester to match up replies to outstanding queries
   *
   * @return the ID for this query
   */
  public int getQueryId() {
    return getHeader().getId();
  }

  @Override
  public String toString() {
    try {
      return isQuery() ? toJSONObjectQuestion().toString() : toJSONObject().toString();
    } catch (JSONException e) {
      return "DNSPacket{" + "header=" + getHeader() + ", guid=" + getGuid() + ", key=" + getKey() + '}';
    }
  }

  /**
   * Return the header.
   * 
   * @return the header
   */
  public Header getHeader() {
    return header;
  }

  /**
   * @param header the header to set
   */
  public void setHeader(Header header) {
    this.header = header;
  }

  /**
   * Return the guid.
   * 
   * @return the guid
   */
  public String getGuid() {
    return guid;
  }

  /**
   * Return the key.
   * 
   * @return the key
   */
  public String getKey() {
    return key;
  }

  /**
   * Return true if the field key is ALL_FIELDS or a top level key.
   * 
   * @return true or false
   */
  public boolean keyIsAllFieldsOrTopLevel() {
    if (key != null) {
      return FieldAccess.isKeyAllFieldsOrTopLevel(key);
    } else {
      return false;
    }
  }

  /**
   * Returns the keys in a multi-field query.
   *
   * @return all the keys
   */
  public List<String> getKeys() {
    return keys;
  }

  /**
   * Return the key or multiple keys.
   * 
   * @return the key or multiple keys
   */
  public String getKeyOrKeysString() {
    return key != null ? key : keys.toString();
  }

  /**
   * Return the TTL
   * 
   * @return the ttl
   */
  public int getTTL() {
    return ttl;
  }

  /**
   * Set the ttl.
   * 
   * @param ttl the ttl to set
   */
  public void setTTL(int ttl) {
    this.ttl = ttl;
  }

  /**
   * Return the record value.
   * 
   * @return the record value
   */
  public ValuesMap getRecordValue() {
    return recordValue;
  }

  /**
   * Set the record value.
   * 
   * @param recordValue
   */
  public void setRecordValue(ValuesMap recordValue) {
    this.recordValue = recordValue;
  }

  /**
   * Set the single return value.
   * 
   * @param data the data to set
   */
  public void setSingleReturnValue(ResultValue data) {
    if (this.recordValue == null) {
      this.recordValue = new ValuesMap();
    }
    this.recordValue.putAsArray(key, data);
  }

  /**
   * Return the source id.
   * 
   * @return the source id
   */
  public NodeIDType getSourceId() {
    return sourceId;
  }

  /**
   * Set the source id.
   * 
   * @param sourceId
   */
  public void setSourceId(NodeIDType sourceId) {
    this.sourceId = sourceId;
  }

  /**
   * Return the responder.
   * 
   * @return the responder
   */
  public NodeIDType getResponder() {
    return responder;
  }

  /**
   * Set the responder.
   * 
   * @param responder
   */
  public void setResponder(NodeIDType responder) {
    this.responder = responder;
  }

  /**
   * Return the return format.
   * 
   * @return a {@link ColumnFieldType}
   */
  public ColumnFieldType getReturnFormat() {
    return returnFormat;
  }

  // For InterfaceRequest
  @Override
  public String getServiceName() {
    return this.guid;
  }
}
