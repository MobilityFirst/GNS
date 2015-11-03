/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp.packet;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.utils.JSONUtils;
import edu.umass.cs.gns.utils.Util;
import edu.umass.cs.nio.Stringifiable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This packet is sent by a CCP to a name server to add a multiple names to GNS.
 *
 * The packet contains request IDs which are used by local name server, and the client (end-user).
 *
 * A client sending this packet sets an initial values associated with the names.
 *
 * A client must set the <code>requestID</code> field correctly to received a response.
 *
 * Once this packet reaches CCP, local name server sets the
 * <code>localNameServerID</code> and <code>CCPRequestID</code> fields before forwarding packet
 * to name server.
 *
 * When name server replies to the client after adding the record, it uses a different packet type:
 * <code>ConfirmUpdatePacket</code>. But it uses fields in this packet in sending the reply.
 *
 * @param <NodeIDType>
 */
public class AddBatchRecordPacket<NodeIDType> extends AbstractAddRecordPacket<NodeIDType> implements InterfaceRequest {

  private final static String NAMES = "names";
  private final static String VALUES = "values";
  private final static String SERVICE_NAME = "service_name";

  /**
   * Host/domain/device name *
   */
  private final JSONArray names;

  /**
   * A map of the values per name that should be set.
   */
  private final JSONObject values;

  /**
   * The service name.
   */
  private String serviceName; // computed at creation time from the names

  /**
   * Constructs a new AddBatchRecordPacket with the given names and values.
   * This constructor does not specify one fields in this packet: <code>CCPRequestID</code>.
   * <code>CCPRequestID</code> can be set by calling <code>setCCPRequestID</code>.
   *
   * We can also change the <code>localNameServerID</code> field in this packet by calling
   * <code>setLocalNameServerID</code>.
   *
   * @param sourceId
   * @param requestID Unique identifier used by the entity making the initial request to confirm
   * @param names
   * @param values
   * @param lnsAddress
   */
  public AddBatchRecordPacket(NodeIDType sourceId, int requestID, Set<String> names, Map<String, JSONObject> values,
          InetSocketAddress lnsAddress) {
    super(sourceId, requestID, lnsAddress);
    this.type = Packet.PacketType.ADD_BATCH_RECORD;
    this.names = new JSONArray(names);
    this.values = new JSONObject(values);
    try {
      this.serviceName = "BATCH" + this.names.getString(0);
      GNS.getLogger().severe("??????????????????????????? AddBatchRecordPacket SERVICE NAME: " + this.serviceName);
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem getting names from AddBatchRecordPacket: " + e);
      this.serviceName = "BAD" + Util.randomString(6);
    }
  }

  /**
   * Constructs a new AddRecordPacket from a JSONObject
   *
   * @param json JSONObject that represents this packet
   * @param unstringer
   * @throws org.json.JSONException
   */
  public AddBatchRecordPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json, unstringer);
    if (Packet.getPacketType(json) != Packet.PacketType.ADD_BATCH_RECORD) {
      throw new JSONException("AddBatchRecordPacket: wrong packet type " + Packet.getPacketType(json));
    }
    this.names = json.getJSONArray(NAMES);
    this.values = json.getJSONObject(VALUES);
    this.serviceName = json.getString(SERVICE_NAME);
  }

  /**
   * Converts AddRecordPacket object to a JSONObject
   *
   * @return JSONObject that represents this packet
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = super.toJSONObject();
    json.put(NAMES, names);
    json.put(VALUES, values);
    json.put(SERVICE_NAME, serviceName);
    return json;
  }

  /**
   * Return the name.
   *
   * @return the name
   */
  public Set<String> getNames() {
    try {
      return JSONUtils.JSONArrayToSetString(names);
    } catch (JSONException e) {
      // we're basically screwed here
      GNS.getLogger().severe("Problem getting names from AddBatchRecordPacket: " + e);
      return null;
    }
  }

  /**
   * Return the values.
   *
   * @return a JSONObject
   */
  public JSONObject getValues() {
    return values;
  }

  // For InterfaceRequest
  @Override
  public String getServiceName() {
    return serviceName;
  }

}
