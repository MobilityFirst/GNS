package edu.umass.cs.gns.nsdesign.packet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

/**
 * ***********************************************************
 * This class implements the packet transmitted between name servers to inform other of any changes made to the active
 * nameserver set
 *
 * @author Hardeep Uppal ********************************************************
 */
public class ActiveNSUpdatePacket extends BasicPacket {

  public final static String NAMESERVER_ID = "sendingPrimary";
  public final static String ACTIVE_NS_ADDED = "activeNSAdded";
  public final static String ACTIVE_NS_REMOVED = "activeNSRemoved";
  //public final static String RECORDKEY = "recordKey";
  public final static String NAME = "name";
  /**
   * Name (service/host/domain or device name) *
   */
  private String name;
//  /**
//   * The key of the value key pair. For GNRS this will be EdgeRecord, CoreRecord or GroupRecord. *
//   */
//  private NameRecordKey recordKey;
  /**
   * Primary Nameserver sending the update *
   */
  private int nameserverID;
  /**
   * Set containing ids of active nameservers added for this name*
   */
  private Set<Integer> activeNameServerAdded;
  /**
   * Set containing ids of active nameservers removed for this name *
   */
  private Set<Integer> activeNameServerRemoved;

  /**
   * ***********************************************************
   * Constructs new ActiveNSUpdatePacket with the give parameter.
   *
   * @param nameserverID Nameserver sending the update
   * @param name Host/domain/device name
   * @param activeNameServerAdded Set containing ids of active nameservers added for this name.
   * @param activeNameServerRemoved Set containing ids of active nameservers removed for this name.
   * **********************************************************
   */
  public ActiveNSUpdatePacket(int nameserverID, String name, Set<Integer> activeNameServerAdded,
          Set<Integer> activeNameServerRemoved) {
    this.type = Packet.PacketType.ACTIVE_NAMESERVER_UPDATE;
    this.nameserverID = nameserverID;
    //this.recordKey = recordKey;
    this.name = name;
    this.activeNameServerAdded = activeNameServerAdded;
    this.activeNameServerRemoved = activeNameServerRemoved;
  }

  /**
   * ***********************************************************
   * Constructs new ActiveNSUpdatePacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException **********************************************************
   */
  public ActiveNSUpdatePacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.ACTIVE_NAMESERVER_UPDATE) {
      Exception e = new Exception("NewReplicaPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.PacketType.ACTIVE_NAMESERVER_UPDATE;
    this.nameserverID = json.getInt(NAMESERVER_ID);
    //this.recordKey = NameRecordKey.valueOf(json.getString(RECORDKEY));
    this.name = json.getString(NAME);
    this.activeNameServerAdded = toSetInteger(json.getJSONArray(ACTIVE_NS_ADDED));
    this.activeNameServerRemoved = toSetInteger(json.getJSONArray(ACTIVE_NS_REMOVED));
  }

  /**
   * ***********************************************************
   * Converts a ActiveNSUpdatePacket to a JSONObject.
   *
   * @return JSONObject representing this packet.
   * @throws org.json.JSONException **********************************************************
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(NAMESERVER_ID, getNameserverID());
    //json.put(RECORDKEY, getRecordKey().getName());
    json.put(NAME, getName());
    json.put(ACTIVE_NS_ADDED, new JSONArray(getActiveNameServerAdded()));
    json.put(ACTIVE_NS_REMOVED, new JSONArray(getActiveNameServerRemoved()));

    return json;
  }

  /**
   * **********************************************************
   * Converts a JSONArray to an Set of Integers
   *
   * @param json JSONArray
   * @return Set<Integer> with the content of JSONArray.
   * @throws org.json.JSONException *********************************************************
   */
  private Set<Integer> toSetInteger(JSONArray json) throws JSONException {
    Set<Integer> set = new HashSet<Integer>();

    if (json == null) {
      return set;
    }

    for (int i = 0; i < json.length(); i++) {
      set.add(new Integer(json.getString(i)));
    }

    return set;
  }

  /**
   * Test *
   */
  public static void main(String[] args) throws Exception {
    Set<Integer> added = new HashSet<Integer>();
    added.add(1);
    added.add(2);
    added.add(3);
    Set<Integer> removed = new HashSet<Integer>();
    removed.add(4);
    removed.add(5);

    ActiveNSUpdatePacket n = new ActiveNSUpdatePacket(0, "google.com", added, removed);
    System.out.println(n.toJSONObject().toString());

    JSONObject j = n.toJSONObject();
    n = new ActiveNSUpdatePacket(j);
    System.out.println(n.toString());
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the recordKey
   */
//  public NameRecordKey getRecordKey() {
//    return recordKey;
//  }

  /**
   * @return the nameserverID
   */
  public int getNameserverID() {
    return nameserverID;
  }

  /**
   * @return the activeNameServerAdded
   */
  public Set<Integer> getActiveNameServerAdded() {
    return activeNameServerAdded;
  }

  /**
   * @return the activeNameServerRemoved
   */
  public Set<Integer> getActiveNameServerRemoved() {
    return activeNameServerRemoved;
  }
}
