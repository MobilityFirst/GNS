package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * TODO write documentation for this class.
 */
public class NewActiveSetStartupPacket extends BasicPacket {

  private final static String ID = "nasID"; // new active set ID
  private final static String NAME = "name";
  private final static String PRIMARY_SENDER = "primarySender";
  private final static String ACTIVE_SENDER = "activeSender";
  private final static String NEW_ACTIVES = "newActives";
  private final static String OLD_ACTIVES = "oldActives";
  private final static String NEW_ACTIVE_PAXOS_ID = "newPaxosID";
  private final static String OLD_ACTIVE_PAXOS_ID = "oldPaxosID";
  private final static String PREVIOUS_VALUE = "previousValue";
  private final static String PREVIOUS_VALUE_CORRECT = "pvCorrect";
  private final static String TIME_TO_LIVE = "ttlAddress";

  /**
   * A unique ID to distinguish this packet at active replica
   */
  int uniqueID;
  /**
   * name for which the proposal is being done.
   */
  String name;
  /**
   * primary node that sent this message
   */
  int primarySender;
  /**
   * active node to which this message was sent
   */
  int activeSender;
  /**
   * current set of actives of this node.
   */
  Set<Integer> newActives;
  /**
   * Previous set of actives of this node.
   */
  Set<Integer> oldActives;
  /**
   * Paxos ID of the new set of actives.
   */
  String newActivePaxosID;
  /**
   * Paxos ID of the old set of actives.
   */
  String oldActivePaxosID;
  /**
   * Value at the end of previous epoch.
   */
  ValuesMap previousValue; // CAN BE NULL
  /**
   * Value at the end of previous epoch.
   */
  boolean previousValueCorrect;

  /**
   * Time interval (in seconds) that the record may be cached before it should be discarded
   */
  private int ttl = 0;

  /**
   *
   * @param name
   * @param primarySender
   * @param newActives
   */
  public NewActiveSetStartupPacket(String name, //NameRecordKey recordKey, 
          int primarySender, int activeSender, Set<Integer> newActives,
          Set<Integer> oldActives, String oldActivePaxosID, String newActivePaxosID,
          PacketType type1, ValuesMap previousValue, boolean previousValueCorrect) {
    Random r = new Random();
    this.uniqueID = r.nextInt();
    this.name = name;
    //this.recordKey = recordKey;
    this.type = type1;

    this.primarySender = primarySender;
    this.activeSender = activeSender;
    this.newActives = newActives;
    this.oldActives = oldActives;
    this.oldActivePaxosID = oldActivePaxosID;
    this.newActivePaxosID = newActivePaxosID;
    this.previousValue = previousValue;
    this.previousValueCorrect = previousValueCorrect;
  }

  public NewActiveSetStartupPacket(JSONObject json) throws JSONException {

    this.type = Packet.getPacketType(json);

    this.uniqueID = json.getInt(ID);

    this.name = json.getString(NAME);

    //this.recordKey = NameRecordKey.valueOf(json.getString(RECORDKEY));

    this.primarySender = json.getInt(PRIMARY_SENDER);

    this.activeSender = json.getInt(ACTIVE_SENDER);

    String actives = json.getString(NEW_ACTIVES);

    this.newActives = new HashSet<Integer>();

    String[] activeSplit = actives.split(":");

    for (String x : activeSplit) {
      newActives.add(Integer.parseInt(x));
    }

    String old_actives = json.getString(OLD_ACTIVES);

    this.oldActives = new HashSet<Integer>();

    String[] tokens = old_actives.split(":");

    for (String x : tokens) {
      oldActives.add(Integer.parseInt(x));
    }

    this.oldActivePaxosID = json.getString(OLD_ACTIVE_PAXOS_ID);

    this.newActivePaxosID = json.getString(NEW_ACTIVE_PAXOS_ID);

    this.previousValue = json.has(PREVIOUS_VALUE) ? new ValuesMap(json.getJSONObject(PREVIOUS_VALUE)) : null;
    //this.previousValue = new QueryResultValue(JSONUtils.JSONArrayToArrayList(json.getJSONArray(PREVIOUS_VALUE)));

    this.previousValueCorrect = json.getBoolean(PREVIOUS_VALUE_CORRECT);

    this.ttl = json.getInt(TIME_TO_LIVE);
  }

  /**
   * JSON object that is implemented.
   *
   * @return
   * @throws JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(ID, uniqueID);
    json.put(NAME, name);
    //json.put(RECORDKEY, recordKey.getName());
    json.put(PRIMARY_SENDER, primarySender);
    json.put(ACTIVE_SENDER, activeSender);

    String actives = convertArrayToString(newActives);
    json.put(NEW_ACTIVES, actives);

    String old_actives = convertArrayToString(oldActives);
    json.put(OLD_ACTIVES, old_actives);

    json.put(NEW_ACTIVE_PAXOS_ID, this.newActivePaxosID);
    json.put(OLD_ACTIVE_PAXOS_ID, this.oldActivePaxosID);
    if (previousValue != null) {
      json.put(PREVIOUS_VALUE, previousValue.toJSONObject());
    }
    //json.put(PREVIOUS_VALUE, new JSONArray(previousValue));
    json.put(PREVIOUS_VALUE_CORRECT, previousValueCorrect);
    json.put(TIME_TO_LIVE, getTTL());
    return json;
  }

  private String convertArrayToString(Set<Integer> values) {
    StringBuilder sb = new StringBuilder();
    for (Integer x : values) {
      if (sb.length() == 0) {
        sb.append(x);
      } else {
        sb.append(":" + x);
      }
    }
    return sb.toString();
  }

  public int getID() {
    return uniqueID;
  }

  public String getName() {
    return name;
  }

//	public NameRecordKey getRecordKey() {
//		return recordKey;
//	}
  public int getSendingPrimary() {
    return primarySender;
  }

  public int getSendingActive() {
    return activeSender;
  }

  /**
   *
   * @return
   */
  public Set<Integer> getNewActiveNameServers() {
    return newActives;
  }

  /**
   *
   * @return
   */
  public Set<Integer> getOldActiveNameServers() {
    return oldActives;
  }

  public String getOldActivePaxosID() {
    return oldActivePaxosID;
  }

  public String getNewActivePaxosID() {
    return newActivePaxosID;
  }

  public ValuesMap getPreviousValue() {
    return previousValue;
  }

  public boolean getPreviousValueCorrect() {
    return previousValueCorrect;
  }

  public void changePacketTypeToForward() {
    setType(PacketType.NEW_ACTIVE_START_FORWARD);
  }

  public void changePacketTypeToResponse() {
    setType(PacketType.NEW_ACTIVE_START_RESPONSE);
  }

  public void changePacketTypeToConfirmation() {
    setType(PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY);
  }

  public void changePacketTypeToPreviousValueRequest() {
    setType(PacketType.NEW_ACTIVE_START_PREV_VALUE_REQUEST);
  }

  public void changePacketTypeToPreviousValueResponse() {
    setType(PacketType.NEW_ACTIVE_START_PREV_VALUE_RESPONSE);
  }

  public void changeSendingActive(int ID) {
    this.activeSender = ID;
  }

  public void changePreviousValue(ValuesMap value) {
    this.previousValue = value;
  }

  public void changePreviousValueCorrect(boolean previousValueCorrect) {
    this.previousValueCorrect = previousValueCorrect;
  }

  public void setTTL(int ttl) {
    this.ttl = ttl;
  }

  public int getTTL() {
    return ttl;
  }
}
