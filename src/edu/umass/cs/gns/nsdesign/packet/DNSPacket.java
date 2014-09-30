/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.clientsupport.FieldAccess;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Set;

/**
 **
 * Packet transmitted between the local name server and a name server. All communications inside of the domain protocol are carried
 * in a single DNS packet. The packet contains the query from a local name server and a response from the name server.
 *
 *
 */
public class DNSPacket extends BasicPacketWithSignatureInfoAndLnsAddress {

  private final static String HEADER = "dns_header";
  private final static String GUID = "dns_guid";
  private final static String KEY = "dns_key";
  private final static String KEYS = "dns_keys";
  private final static String TIME_TO_LIVE = "ttlAddress";
  private final static String RECORD_VALUE = "recordValue";
  private final static String SOURCE_ID = "sourceId";
  private final static String RESPONDER = "rspndr";
  private final static String RETURN_FORMAT = "format";

  /**
   *
   * This is the source ID of a packet that should be returned to the intercessor of the LNS.
   * Otherwise the sourceId field contains the number of the NS who made the request.
   */
  public final static NodeId<String> LOCAL_SOURCE_ID = GNSNodeConfig.INVALID_NAME_SERVER_ID;
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
  private String guid;
  /**
   * The key of the value key pair. Mutually exclusive with keys.
   */
  private final String key;
  /**
   * The keys when we are doing a multiple field query lookup. Mutually exclusive with key.
   */
  private final ArrayList<String> keys;
  /**
   * This is the id of the source of the request, -1 (AKA LOCAL_SOURCE_ID) means the client is the intercessor
   * of the LNS handling the request. Otherwise it will be the ID of a NameServer.
   */
  private NodeId<String> sourceId;
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
  ColumnFieldType returnFormat;
  /**
   * For response packets this is the node that responded
   */
  private NodeId<String> responder = GNSNodeConfig.INVALID_NAME_SERVER_ID;

  /**
   * Constructs a packet for querying a name server for name information.
   *
   * @param sourceId - the id of the server sending the request, equal to LOCAL_SOURCE_ID means the intercessor of the LNS (the usual case)
   * @param id
   * @param guid
   * @param key
   * @param accessor
   * @param signature
   * @param message
   * @param returnFormat
   */
  public DNSPacket(NodeId<String> sourceId, int id, String guid, String key, ArrayList<String> keys,
          ColumnFieldType returnFormat,
          String accessor, String signature, String message) {
    super(null, accessor, signature, message); // lnsAddress is null
    this.header = new Header(id, DNSRecordType.QUERY, NSResponseCode.NO_ERROR);
    this.guid = guid;
    this.key = key;
    this.keys = keys;
    this.sourceId = sourceId;
    this.responder = GNSNodeConfig.INVALID_NAME_SERVER_ID;
    this.returnFormat = returnFormat;
  }

  /**
   **
   * Constructs a packet from a JSONObject that represents a DNS packet.
   * This packet has both a query and response section.
   * The response section will be empty if this is an error response.
   *
   * @param json JSONObject that represents a DNS packet
   * @throws org.json.JSONException
   */
  public DNSPacket(JSONObject json) throws JSONException {
    super(json.optString(LNS_ADDRESS, null), json.optInt(LNS_PORT, INVALID_PORT),
            json.optString(ACCESSOR, null), json.optString(SIGNATURE, null), json.optString(MESSAGE, null));
    //super(json.optString(ACCESSOR, null), json.optString(SIGNATURE, null), json.optString(MESSAGE, null));
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
    this.sourceId = new NodeId<String>(json.getString(SOURCE_ID));
    // read the optional responder if it is there
    this.responder = json.has(RESPONDER) ? new NodeId<String>(json.getString(RESPONDER)) : GNSNodeConfig.INVALID_NAME_SERVER_ID;
    this.returnFormat = json.has(RETURN_FORMAT) ? ColumnFieldType.valueOf(json.getString(RETURN_FORMAT)) : null;

    // These will only be present in non-error response packets
    if (header.isResponse() && !header.isAnyKindOfError()) {
      this.ttl = json.getInt(TIME_TO_LIVE);
      if (json.has(RECORD_VALUE)) {
        this.recordValue = new ValuesMap(json.getJSONObject(RECORD_VALUE));
      }
    }

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
  public DNSPacket(NodeId<String> sourceId, int id, String name, String key, ResultValue fieldValue, int TTL, Set<Integer> activeNameServers) {
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
   * @param entireRecord
   * @param TTL
   * @param activeNameServers
   */
  public DNSPacket(NodeId<String> sourceId, int id, String name, String key, ArrayList<String> keys, ValuesMap entireRecord, int TTL, Set<Integer> activeNameServers) {
    super(null); // lnsAddress is null and no sigs for this baby
    this.header = new Header(id, DNSRecordType.RESPONSE, NSResponseCode.NO_ERROR);
    this.guid = name;
    this.key = key;
    this.keys = keys;
    this.sourceId = sourceId;
    this.recordValue = entireRecord;
    this.ttl = TTL;
    this.responder = GNSNodeConfig.INVALID_NAME_SERVER_ID;
    this.returnFormat = null;
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
   * @return
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
    // query section
    json.put(HEADER, getHeader().toJSONObject());
    if (key != null) {
      json.put(KEY, key);
    }
    if (keys != null) {
      json.put(KEYS, keys);
    }
    json.put(GUID, guid);
    json.put(SOURCE_ID, sourceId.toString());
    if (returnFormat != null) {
      json.put(RETURN_FORMAT, returnFormat.name());
    }
    // this goes in with query (if it's not empty -1) in case it's an error response and we want to know the reponder
    if (!responder.equals(GNSNodeConfig.INVALID_NAME_SERVER_ID)) {
      json.put(RESPONDER, responder.toString());
    }
    // response section
    if (includeReponseSection) {
      json.put(TIME_TO_LIVE, getTTL());
      if (recordValue != null) {
        json.put(RECORD_VALUE, recordValue);
      }
    }
  }

  /**
   *
   * Returns true if the packet is a query, false otherwise
   *
   * @return
   */
  public boolean isQuery() {
    return getHeader().isQuery();
  }

  /**
   * Returns true if the packet is a response, false otherwise
   *
   * @return
   */
  public boolean isResponse() {
    return getHeader().isResponse();
  }

  /**
   **
   * Returns true if the packet contains a response error, false otherwise
   *
   * @return
   */
  public boolean containsAnyError() {
    return getHeader().isAnyKindOfError();
  }

  public boolean containsInvalidActiveNSError() {
    return getHeader().isInvalidActiveNSError();
  }

  /**
   **
   * Returns the ID for this query from the packet header. Used by the requester to match up replies to outstanding queries
   *
   * @return
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
   * @return the guid
   */
  public String getGuid() {
    return guid;
  }

  /**
   * @return the qrecordKey
   */
  public String getKey() {
    return key;
  }

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
   * @return
   */
  public ArrayList<String> getKeys() {
    return keys;
  }

  public String getKeyOrKeysString() {
    return key != null ? key : keys.toString();
  }

  /**
   * @return the ttlAddress
   */
  public int getTTL() {
    return ttl;
  }

  /**
   * @param ttlAddress the ttlAddress to set
   */
  public void setTTL(int ttlAddress) {
    this.ttl = ttlAddress;
  }

  /**
   *
   * @return
   */
  public ValuesMap getRecordValue() {
    return recordValue;
  }

  /**
   *
   * @param recordValue
   */
  public void setRecordValue(ValuesMap recordValue) {
    this.recordValue = recordValue;
  }

  /**
   * @param data the data to set
   */
  public void setSingleReturnValue(ResultValue data) {
    if (this.recordValue == null) {
      this.recordValue = new ValuesMap();
    }
    this.recordValue.putAsArray(key, data);
  }

  public NodeId<String> getSourceId() {
    return sourceId;
  }

  public void setSourceId(NodeId<String> sourceId) {
    this.sourceId = sourceId;
  }

  public NodeId<String> getResponder() {
    return responder;
  }

  public void setResponder(NodeId<String> responder) {
    this.responder = responder;
  }

  public ColumnFieldType getReturnFormat() {
    return returnFormat;
  }

}
