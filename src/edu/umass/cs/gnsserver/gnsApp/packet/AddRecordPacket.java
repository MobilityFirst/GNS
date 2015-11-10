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
import edu.umass.cs.gnsserver.utils.JSONUtils;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.nio.interfaces.Stringifiable;

import java.net.InetSocketAddress;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This packet is sent by a CCP to a name server to add a name to GNS.
 *
 * The packet contains request IDs which are used by local name server, and the client (end-user).
 *
 * A client sending this packet sets an initial key/fieldValue pair associated with the name or
 * the entire JSONObject associated with the name.
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
public class AddRecordPacket<NodeIDType> extends AbstractAddRecordPacket<NodeIDType> implements Request {

  private final static String NAME = "name";
  private final static String FIELD = "field";
  private final static String FIELDVALUE = "fieldValue";
  private final static String VALUES = "values";

  /**
   * Host/domain/device name *
   */
  private final String name;

  /**
   * The key of the fieldValue key pair. *
   */
  private final String field;

  /**
   * A value for the single field that should be set. Mutually exclusive with values. *
   */
  private final ResultValue fieldValue;

  /**
   * A record value that should be set. Mutually exclusive with fieldValue. *
   */
  private final JSONObject values;

  /**
   * Constructs a new AddRecordPacket with the given name, fieldValue.
   * This constructor does not specify one fields in this packet: <code>CCPRequestID</code>.
   * <code>CCPRequestID</code> can be set by calling <code>setCCPRequestID</code>.
   *
   * We can also change the <code>localNameServerID</code> field in this packet by calling
   * <code>setLocalNameServerID</code>.
   *
   * @param sourceId
   * @param requestID Unique identifier used by the entity making the initial request to confirm
   * @param name Host/domain/device name
   * @param field The initial key that will be stored in the name record.
   * @param fieldValue The inital fieldValue of the key that is specified.
   * @param lnsAddress
   */
  public AddRecordPacket(NodeIDType sourceId, int requestID, String name, String field,
          ResultValue fieldValue, InetSocketAddress lnsAddress) {
    super(sourceId, requestID, lnsAddress);
    this.type = Packet.PacketType.ADD_RECORD;
    this.name = name;
    this.values = null;
    this.field = field;
    this.fieldValue = fieldValue;
  }

  /**
   * Constructs a new AddRecordPacket with the given name and values.
   *
   * @param sourceId
   * @param requestID
   * @param name
   * @param values
   * @param lnsAddress
   */
  public AddRecordPacket(NodeIDType sourceId, int requestID, String name,
          JSONObject values, InetSocketAddress lnsAddress) {
    super(sourceId, requestID, lnsAddress);
    this.type = Packet.PacketType.ADD_RECORD;
    this.name = name;
    this.values = values;
    this.field = null;
    this.fieldValue = null;
  }

  /**
   * Constructs a new AddRecordPacket from a JSONObject
   *
   * @param json JSONObject that represents this packet
   * @param unstringer
   * @throws org.json.JSONException
   */
  public AddRecordPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json, unstringer);
    if (Packet.getPacketType(json) != Packet.PacketType.ADD_RECORD //&& Packet.getPacketType(json) != Packet.PacketType.ACTIVE_ADD
            //&& Packet.getPacketType(json) != Packet.PacketType.ACTIVE_ADD_CONFIRM
            ) {
      throw new JSONException("AddRecordPacket: wrong packet type " + Packet.getPacketType(json));
    }
    this.name = json.getString(NAME);
    this.field = json.optString(FIELD, null);
    if (json.has(FIELDVALUE)) {
      this.fieldValue = JSONUtils.JSONArrayToResultValue(json.getJSONArray(FIELDVALUE));
    } else {
      this.fieldValue = null;
    }
    this.values = json.optJSONObject(VALUES);
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
    json.put(NAME, getName());
    if (field != null) {
      json.put(FIELD, field);
    }
    if (fieldValue != null) {
      json.put(FIELDVALUE, new JSONArray(fieldValue));
    }
    if (values != null) {
      json.put(VALUES, values);
    }
    return json;
  }

  /**
   * Return the name.
   *
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Return the field.
   *
   * @return the field
   */
  public String getField() {
    return field;
  }

  /**
   * Return the field value.
   *
   * @return the fieldValue
   */
  public ResultValue getFieldValue() {
    return fieldValue;
  }

  // For InterfaceRequest
  @Override
  public String getServiceName() {
    return this.name;
  }

  /**
   * Return the values.
   *
   * @return a JSONObject
   */
  public JSONObject getValues() {
    return values;
  }

}
