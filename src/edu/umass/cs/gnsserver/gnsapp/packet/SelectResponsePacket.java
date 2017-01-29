
package edu.umass.cs.gnsserver.gnsapp.packet;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.interfaces.Stringifiable;

import java.net.InetSocketAddress;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class SelectResponsePacket<NodeIDType> extends BasicPacketWithReturnAddressAndNs<NodeIDType> implements ClientRequest {

  //
  private final static String ID = "id";
  private final static String RECORDS = "records";
  private final static String GUIDS = "guids";
  private final static String LNSQUERYID = "lnsQueryId";
  private final static String NSQUERYID = "nsQueryId";
  private final static String RESPONSECODE = "code";
  private final static String ERRORSTRING = "error";
  


  public enum ResponseCode {

    NOERROR, 

    ERROR

  }

  private long requestId;
  private int nsQueryId;
  private JSONArray records;
  private JSONArray guids;
  private ResponseCode responseCode;
  private String errorMessage;


  private SelectResponsePacket(long id, InetSocketAddress lnsAddress, int nsQueryId,
          NodeIDType nameServerID, JSONArray records, JSONArray guids, ResponseCode responseCode,
          String errorMessage) {
    super(nameServerID, lnsAddress);
    this.type = Packet.PacketType.SELECT_RESPONSE;
    this.requestId = id;
    this.nsQueryId = nsQueryId;
    this.records = records;
    this.guids = guids;
    this.responseCode = responseCode;
    this.errorMessage = errorMessage;
  }


  public static SelectResponsePacket<String> makeSuccessPacketForRecordsOnly(long id, InetSocketAddress lnsAddress,
          long lnsQueryId,
          int nsQueryId, String nameServerID, JSONArray records) {
    return new SelectResponsePacket<>(id, lnsAddress, nsQueryId, nameServerID, records, null,
            ResponseCode.NOERROR, null);
  }


  public static SelectResponsePacket<String> makeSuccessPacketForGuidsOnly(long id,
          InetSocketAddress lnsAddress,
          int nsQueryId, String nameServerID, JSONArray guids) {
    return new SelectResponsePacket<>(id, lnsAddress, nsQueryId, nameServerID,
            null, guids, ResponseCode.NOERROR, null);
  }


  public static SelectResponsePacket<String> makeFailPacket(long id, InetSocketAddress lnsAddress,
           int nsQueryId, String nameServer, String errorMessage) {
    return new SelectResponsePacket<>(id, lnsAddress, nsQueryId, nameServer,
            null, null, ResponseCode.ERROR, errorMessage);
  }


  public SelectResponsePacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json, unstringer);
    if (Packet.getPacketType(json) != Packet.PacketType.SELECT_RESPONSE) {
      throw new JSONException("StatusPacket: wrong packet type " + Packet.getPacketType(json));
    }
    this.type = Packet.getPacketType(json);
    this.requestId = json.getLong(ID);
    //this.lnsID = json.getInt(LNSID);
    this.nsQueryId = json.getInt(NSQUERYID);
    //this.nameServer = new NodeIDType(json.getString(NAMESERVER));
    this.responseCode = ResponseCode.valueOf(json.getString(RESPONSECODE));
    // either of these could be null
    this.records = json.optJSONArray(RECORDS);
    this.guids = json.optJSONArray(GUIDS);
    this.errorMessage = json.optString(ERRORSTRING, null);

  }


  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    super.addToJSONObject(json);
    json.put(ID, requestId);
    //json.put(LNSID, lnsID);
    json.put(NSQUERYID, nsQueryId);
    //json.put(NAMESERVER, nameServer.toString());
    json.put(RESPONSECODE, responseCode.name());
    if (records != null) {
      json.put(RECORDS, records);
    }
    if (guids != null) {
      json.put(GUIDS, guids);
    }
    if (errorMessage != null) {
      json.put(ERRORSTRING, errorMessage);
    }
    return json;
  }


  public long getId() {
    return requestId;
  }


  public JSONArray getRecords() {
    return records;
  }


  public JSONArray getGuids() {
    return guids;
  }


  public int getNsQueryId() {
    return nsQueryId;
  }


  public ResponseCode getResponseCode() {
    return responseCode;
  }


  public String getErrorMessage() {
    return errorMessage;
  }


  @Override
  public String getServiceName() {
    // FIXME:
    return "SelectResponse";
  }


  @Override
  public ClientRequest getResponse() {
    return this.response;
  }


  @Override
  public long getRequestID() {
    return requestId;
  }
}
