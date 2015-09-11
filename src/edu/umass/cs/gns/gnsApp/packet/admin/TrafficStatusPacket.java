package edu.umass.cs.gns.gnsApp.packet.admin;

import edu.umass.cs.gns.main.GNS.PortType;
import edu.umass.cs.gns.gnsApp.packet.BasicPacket;
import edu.umass.cs.gns.gnsApp.packet.Packet;
import edu.umass.cs.gns.gnsApp.packet.Packet.PacketType;
import edu.umass.cs.gns.utils.Format;
import org.json.JSONException;
import org.json.JSONObject;

import java.text.ParseException;
import java.util.Date;

/**
 *
 * This class implements a packet that contains sends traffic status information
 *
 * @author Westy 
 * @param <NodeIDType> 
 * 
 */
public class TrafficStatusPacket<NodeIDType> extends BasicPacket {

  /** fromID */
  public final static String FROMID = "fromID";

  /** toID */
  public final static String TOID = "toID";

  /** porttype */
  public final static String PORTTYPE = "porttype";

  /** packettype */
  public final static String PACKETTYPE = "packettype";

  /** time */
  public final static String TIME = "time";

  /** name */
  public final static String NAME = "name";

  /** other */
  public final static String OTHER = "other";
  private Date time;
  private NodeIDType fromID;
  private NodeIDType toID;
  private PortType portType;
  private PacketType packetType;
  private String name;
  private String other;

  /**
   * Constructs a new status packet
   *
   * @param fromID
   * @param toID
   * @param portType
   */
//  public TrafficStatusPacket(int fromID, int toID, PortType portType, Packet.PacketType packetType) {
//    this(fromID, toID, portType, packetType, null, null, null);
//  }
  /**
   * Constructs a new TrafficStatusPacket packet
   * @param fromID
   * @param toID
   * @param portType
   * @param packetType
   * @param name
   * @param other
   */
  public TrafficStatusPacket(NodeIDType fromID, NodeIDType toID, PortType portType, PacketType packetType, String name,
          // String key,
          String other) {
    this.type = PacketType.TRAFFIC_STATUS;
    this.time = new Date();
    this.fromID = fromID;
    this.toID = toID;
    this.portType = portType;
    this.packetType = packetType;
    this.name = name;
    this.other = other;
  }

  /**
   *
   * Constructs new TrafficStatusPacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   * @throws java.text.ParseException
   *
   */
  @SuppressWarnings("unchecked")
  public TrafficStatusPacket(JSONObject json) throws JSONException, ParseException {
    if (Packet.getPacketType(json) != PacketType.TRAFFIC_STATUS) {
      Exception e = new Exception("TrafficStatusPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.getPacketType(json);
    this.time = Format.parseDateTimeOnlyMilleUTC(json.getString(TIME));
    this.fromID = (NodeIDType) json.get(FROMID);
    this.toID = (NodeIDType) json.get(TOID);
    this.portType = PortType.valueOf(json.getString(PORTTYPE));
    this.packetType = PacketType.valueOf(json.getString(PACKETTYPE));
    this.name = json.optString(NAME, null);
    this.other = json.optString(OTHER, null);
  }

  /**
   * Return the time.
   * 
   * @return the time
   */
  public Date getTime() {
    return time;
  }

  /**
   * Return the from id.
   * 
   * @return the from id
   */
  public NodeIDType getFromID() {
    return fromID;
  }

  /**
   * Return the to id.
   * 
   * @return the to id
   */
  public NodeIDType getToID() {
    return toID;
  }

  /**
   * Return the port type.
   * 
   * @return the port type
   */
  public PortType getPortType() {
    return portType;
  }

  /**
   * Return the packet type.
   * 
   * @return the packet type
   */
  public PacketType getPacketType() {
    return packetType;
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
   * Return the other.
   * 
   * @return the other
   */
  public String getOther() {
    return other;
  }

  /**
   * Converts a TrafficStatusPacket to a JSONObject.
   *
   * @return JSONObject representing this packet.
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(TIME, Format.formatDateTimeOnlyMilleUTC(time));
    json.put(FROMID, fromID.toString());
    json.put(TOID, toID.toString());
    json.put(PORTTYPE, portType.name());
    json.put(PACKETTYPE, packetType.name());
    if (name != null) {
      json.put(NAME, name);
    }
    if (other != null) {
      json.put(OTHER, other);
    }
    return json;
  }
}
