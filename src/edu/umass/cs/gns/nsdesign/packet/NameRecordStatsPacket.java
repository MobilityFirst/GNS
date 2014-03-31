package edu.umass.cs.gns.nsdesign.packet;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * 
 * This class implements a packet used to aggregate read and write frequency of a name from an active nameserver. A
 * primary nameserver makes a request for stats to an active nameserver which responds back with its current read and
 * write frequency.
 *
 * Abhigyan: we are not using this packet because current implementation is using local name server to sending read
 * and write stats to name servers. I am not deleting this file because as per system design the task of
 * sending read and write rates is done by active name servers. However, currently we don't have any way of inferring
 * locality of demand at active name servers, so local name servers are doing this task.
 *
 * @author Hardeep Uppal
 
 */
public class NameRecordStatsPacket extends BasicPacket {

  private final static String NAME = "name";
  private final static String READ_FREQUENCY = "read";
  private final static String WRITE_FREQUENCY = "write";
  private final static String ACTIVE_NAMESERVER_ID = "active";
  //
  /**
   * Host/domain/device name *
   */
  private String name;
  /**
   * Read frequency of this name *
   */
  private int readFrequency;
  /**
   * Write frequency of this name *
   */
  private int writeFrequency;
  /**
   * Id of the active nameserver sending the response *
   */
  private int activeNameServerId;

  /**
   *
   * Constructs a new NameRecordStatsPacket with the given parameters.
   *
   * @param ptype Packet type
   * @param name Host/domain/device name
   * @param readFrequency Read frequency of this name
   * @param writeFrequency Write frequency of this name
   * @param activeNameServerId Id of the active nameserver sending the response

   */
  public NameRecordStatsPacket(//NameRecordKey recordKey,
          String name, int readFrequency,
          int writeFrequency, int activeNameServerId) {
    this.type = Packet.PacketType.NAME_RECORD_STATS_RESPONSE;
    //this.recordKey = recordKey;
    this.name = name;
    this.readFrequency = readFrequency;
    this.writeFrequency = writeFrequency;
    this.activeNameServerId = activeNameServerId;
  }

  /**
   *
   * Constructs a new NameRecordStatsPacket from a JSONObject.
   *
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException

   */
  public NameRecordStatsPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.NAME_RECORD_STATS_REQUEST
            && Packet.getPacketType(json) != Packet.PacketType.NAME_RECORD_STATS_RESPONSE) {
      Exception e = new Exception("NameRecordStatsPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
    }

    this.type = Packet.getPacketType(json);
    this.name = json.getString(NAME);
    this.readFrequency = json.getInt(READ_FREQUENCY);
    this.writeFrequency = json.getInt(WRITE_FREQUENCY);
    this.activeNameServerId = json.getInt(ACTIVE_NAMESERVER_ID);
  }

  /**
   *
   * Converts NameRecordStatsPacket object to a JSONObject
   *
   * @return JSONObject that represents this packet
   * @throws org.json.JSONException
   
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(NAME, getName());
    json.put(READ_FREQUENCY, getReadFrequency());
    json.put(WRITE_FREQUENCY, getWriteFrequency());
    json.put(ACTIVE_NAMESERVER_ID, getActiveNameServerId());

    return json;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }
  /**
   * @return the readFrequency
   */
  public int getReadFrequency() {
    return readFrequency;
  }

  /**
   * @return the writeFrequency
   */
  public int getWriteFrequency() {
    return writeFrequency;
  }

  /**
   * @return the activeNameServerId
   */
  public int getActiveNameServerId() {
    return activeNameServerId;
  }
}
