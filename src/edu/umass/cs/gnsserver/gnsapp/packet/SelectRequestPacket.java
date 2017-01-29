
package edu.umass.cs.gnsserver.gnsapp.packet;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ShaOneHashFunction;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.nio.interfaces.Stringifiable;

import org.json.JSONException;
import org.json.JSONObject;


public class SelectRequestPacket<NodeIDType> extends BasicPacketWithNs<NodeIDType> implements ClientRequest {

  private final static String ID = "id";
  private final static String KEY = "key";
  private final static String VALUE = "value";
  private final static String OTHERVALUE = "otherValue";
  private final static String QUERY = "query";
  private final static String CCPQUERYID = "ccpQueryId";
  private final static String NSQUERYID = "nsQueryId";
  private final static String SELECT_OPERATION = "operation";
  private final static String GROUP_BEHAVIOR = "group";
  private final static String GUID = "guid";
  private final static String REFRESH = "refresh";
  //
  private long requestId;
  private String key;
  private Object value;
  private Object otherValue;
  private String query;
  private int ccpQueryId = -1; // used by the command processor to maintain state
  private int nsQueryId = -1; // used by the name server to maintain state
  private SelectOperation selectOperation;
  private SelectGroupBehavior groupBehavior;
  // for group guid
  private String guid; // the group GUID we are maintaning or null for simple select
  private int minRefreshInterval; // minimum time between allowed refreshes of the guid


  @SuppressWarnings("unchecked")
  public SelectRequestPacket(long id, SelectOperation selectOperation, SelectGroupBehavior groupBehavior,
          String key, Object value, Object otherValue) {
    super(null);
    this.type = Packet.PacketType.SELECT_REQUEST;
    this.requestId = id;
    this.key = key;
    this.value = value;
    this.otherValue = otherValue;
    this.selectOperation = selectOperation;
    this.groupBehavior = groupBehavior;
    this.query = null;
    this.guid = null;
  }


  @SuppressWarnings("unchecked")
  private SelectRequestPacket(long id, SelectOperation selectOperation, SelectGroupBehavior groupOperation,
          String query, String guid, int minRefreshInterval) {
    super(null);
    this.type = Packet.PacketType.SELECT_REQUEST;
    this.requestId = id;
    this.query = query;
    this.selectOperation = selectOperation;
    this.groupBehavior = groupOperation;
    this.key = null;
    this.value = null;
    this.otherValue = null;
    this.guid = guid;
    this.minRefreshInterval = minRefreshInterval;
  }


  public static SelectRequestPacket<String> MakeQueryRequest(long id, String query) {
    return new SelectRequestPacket<>(id, SelectOperation.QUERY, SelectGroupBehavior.NONE, query, null, -1);
  }


  public static SelectRequestPacket<String> MakeGroupSetupRequest(long id, String query, String guid,
          int refreshInterval) {
    return new SelectRequestPacket<>(id, SelectOperation.QUERY, SelectGroupBehavior.GROUP_SETUP,
            query, guid, refreshInterval);
  }


  public static SelectRequestPacket<String> MakeGroupLookupRequest(long id, String guid) {
    return new SelectRequestPacket<>(id, SelectOperation.QUERY, SelectGroupBehavior.GROUP_LOOKUP, null, guid, -1);
  }


  @SuppressWarnings("unchecked")
  public SelectRequestPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json, unstringer);
    if (Packet.getPacketType(json) != Packet.PacketType.SELECT_REQUEST) {
      throw new JSONException("SelectRequestPacket: wrong packet type " + Packet.getPacketType(json));
    }
    this.type = Packet.getPacketType(json);
    this.requestId = json.getLong(ID);
    this.key = json.has(KEY) ? json.getString(KEY) : null;
    this.value = json.optString(VALUE, null);
    this.otherValue = json.optString(OTHERVALUE, null);
    this.query = json.optString(QUERY, null);
    this.ccpQueryId = json.getInt(CCPQUERYID);
    //this.nsID = new NodeIDType(json.getString(NSID));
    this.nsQueryId = json.getInt(NSQUERYID);
    this.selectOperation = SelectOperation.valueOf(json.getString(SELECT_OPERATION));
    this.groupBehavior = SelectGroupBehavior.valueOf(json.getString(GROUP_BEHAVIOR));
    this.guid = json.optString(GUID, null);
    this.minRefreshInterval = json.optInt(REFRESH, -1);
  }


  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    addToJSONObject(json);
    return json;
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    Packet.putPacketType(json, getType());
    super.addToJSONObject(json);
    json.put(ID, requestId);
    if (key != null) {
      json.put(KEY, key);
    }
    if (value != null) {
      json.put(VALUE, value);
    }
    if (otherValue != null) {
      json.put(OTHERVALUE, otherValue);
    }
    if (query != null) {
      json.put(QUERY, query);
    }
    json.put(CCPQUERYID, ccpQueryId);
    //json.put(NSID, nsID.toString());
    json.put(NSQUERYID, nsQueryId);
    json.put(SELECT_OPERATION, selectOperation.name());
    json.put(GROUP_BEHAVIOR, groupBehavior.name());
    if (guid != null) {
      json.put(GUID, guid);
    }
    if (minRefreshInterval != -1) {
      json.put(REFRESH, minRefreshInterval);
    }
  }


  public void setCCPQueryId(int ccpQueryId) {
    this.ccpQueryId = ccpQueryId;
  }


  public void setNsQueryId(int nsQueryId) {
    this.nsQueryId = nsQueryId;
  }


  public long getId() {
    return requestId;
  }


  public void setRequestId(long requestId) {
    this.requestId = requestId;
  }


  public String getKey() {
    return key;
  }


  public Object getValue() {
    return value;
  }


  public int getCcpQueryId() {
    return ccpQueryId;
  }


  public int getNsQueryId() {
    return nsQueryId;
  }


  public SelectOperation getSelectOperation() {
    return selectOperation;
  }


  public SelectGroupBehavior getGroupBehavior() {
    return groupBehavior;
  }


  public Object getOtherValue() {
    return otherValue;
  }


  public String getQuery() {
    return query;
  }


  public void setQuery(String query) {
    this.query = query;
  }


  @Override
  public String getServiceName() {
    if (query != null) {
      // FIXME: maybe cache this
      return Base64.encodeToString(ShaOneHashFunction.getInstance().hash(this.query), false);
    } else {
      // FIXME:
      return "_SelectRequest_";
    }
  }


  public String getGuid() {
    return guid;
  }


  public int getMinRefreshInterval() {
    return minRefreshInterval;
  }


  @Override
  public ClientRequest getResponse() {
    return this.response;
  }


  @Override
  public long getRequestID() {
    return requestId;
  }


  @Override
  public Object getSummary() {
    return new Object() {
      @Override
      public String toString() {
        return SelectRequestPacket.this.getType() + ":"
                + SelectRequestPacket.this.requestId + ":"
                + SelectRequestPacket.this.getQuery() + "[" + SelectRequestPacket.this.getClientAddress() + "]";
      }
    };
  }
}
