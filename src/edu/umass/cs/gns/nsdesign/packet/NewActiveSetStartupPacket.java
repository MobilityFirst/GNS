package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
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
  private final static String NEW_ACTIVE_VERSION = "newVersion";
  private final static String OLD_ACTIVE_VERSION = "oldVersion";
  private final static String PREVIOUS_VALUE = "previousValue";
  private final static String PREVIOUS_VALUE_CORRECT = "pvCorrect";


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
   * Version of the new set of actives.
   */
  short newActiveVersion;
  /**
   * Version of the old set of actives.
   */
  short oldActiveVersion;
  /**
   * Value at the end of previous epoch.
   */
  String previousValue; // CAN BE NULL
  /**
   * Value at the end of previous epoch.
   */
  boolean previousValueCorrect;

  /**
   *
   * @param name
   * @param primarySender
   * @param newActives
   */
  public NewActiveSetStartupPacket(String name,
          int primarySender, int activeSender, Set<Integer> newActives,
          Set<Integer> oldActives, short oldActiveVersion, short newActiveVersion,
          PacketType type1, String previousValue, boolean previousValueCorrect) {
    Random r = new Random();
    this.uniqueID = r.nextInt();
    this.name = name;
    this.type = type1;

    this.primarySender = primarySender;
    this.activeSender = activeSender;
    this.newActives = newActives;
    this.oldActives = oldActives;
    this.oldActiveVersion = oldActiveVersion;
    this.newActiveVersion = newActiveVersion;
    this.previousValue = previousValue;
    this.previousValueCorrect = previousValueCorrect;
  }


  public NewActiveSetStartupPacket(JSONObject json) throws JSONException {

    this.type = Packet.getPacketType(json);

    this.uniqueID = json.getInt(ID);

    this.name = json.getString(NAME);


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

    this.oldActiveVersion = (short)json.getInt(OLD_ACTIVE_VERSION); 

    this.newActiveVersion = (short)json.getInt(NEW_ACTIVE_VERSION);

    this.previousValue = json.has(PREVIOUS_VALUE) ? json.getString(PREVIOUS_VALUE): null;

    this.previousValueCorrect = json.getBoolean(PREVIOUS_VALUE_CORRECT);
  }

  /**
   * JSON object that is implemented.
   *
   * @return
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(ID, uniqueID);
    json.put(NAME, name);
    json.put(PRIMARY_SENDER, primarySender);
    json.put(ACTIVE_SENDER, activeSender);

    String actives = convertArrayToString(newActives);
    json.put(NEW_ACTIVES, actives);

    String old_actives = convertArrayToString(oldActives);
    json.put(OLD_ACTIVES, old_actives);

    json.put(NEW_ACTIVE_VERSION, this.newActiveVersion);
    json.put(OLD_ACTIVE_VERSION, this.oldActiveVersion);
    if (previousValue != null) {
      json.put(PREVIOUS_VALUE, previousValue);
    }
    json.put(PREVIOUS_VALUE_CORRECT, previousValueCorrect);
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


  public void setUniqueID(int uniqueID) {
    this.uniqueID = uniqueID;
  }


  public int getUniqueID() {
    return uniqueID;
  }

  public String getName() {
    return name;
  }

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

  public short getOldActiveVersion() {
    return oldActiveVersion;
  }

  public short getNewActiveVersion() {
    return newActiveVersion;
  }

  public String getPreviousValue() {
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

  public void changePreviousValue(String value) {
    this.previousValue = value;
  }

  public void changePreviousValueCorrect(boolean previousValueCorrect) {
    this.previousValueCorrect = previousValueCorrect;
  }

}
