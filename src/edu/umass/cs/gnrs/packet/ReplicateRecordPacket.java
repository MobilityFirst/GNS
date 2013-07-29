package edu.umass.cs.gnrs.packet;

//import edu.umass.cs.gnrs.nameserver.NameRecord;
import edu.umass.cs.gnrs.nameserver.ValuesMap;
import edu.umass.cs.gnrs.util.JSONUtils;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * ***********************************************************
 * This class implements the packet that contains NameRecord information. This packet is sent to a new active nameserver to
 * replicate name record information. </br> This packet is sent from a primary nameserver of the NameRecord to a new active
 * nameserver.
 *
 * @author Hardeep Uppal
 ***********************************************************
 */
public class ReplicateRecordPacket extends BasicPacket {

  //public final static String RECORDKEY = "recordkey";
  public final static String NAME = "rp_name";
  public final static String TIMETOLIVE = "rp_timeToLive";
  //public final static String VALUESLIST = "valuesList";
  public final static String VALUESMAP = "rp_valuesMap";
  public final static String PRIMARY_NAMESERVERS = "rp_primary";
  public final static String SECONDARY_NAMESERVERS = "rp_active";
  public final static String PRIMARY_NAMESERVER_ID = "rp_sendingPrimary";
  /**
   * Host/domain/device name *
   */
  private String name;
  /**
   * In GNRS usage whether it is an edge, core or group record or something else *
   */
  //private NameRecordKey recordKey;
  /**
   * Time to live for this name record *
   */
  private int timeToLive;
  /**
   * Map of values for this name record *
   */
  private ValuesMap valuesMap;
  //private ArrayList<String> valuesList;
  /**
   * List of primary nameservers for this name record *
   */
  private HashSet<Integer> primaryNameServers;
  /**
   * List of active nameservers for this name record *
   */
  private Set<Integer> activeNameServers;
  /**
   * Id of primary nameserver sending this request *
   */
  private int primaryNameserverId;

  /**
   * ***********************************************************
   * Constructs a new ReplicateRecordPacket with the given parameters.
   *
   * @param name Host/domain/device name
   * @param valuesMap Map of values
   * @param primaryNameServers List of primary nameservers
   * @param activeNameServers List of active nameservers
   ***********************************************************
   */
  private ReplicateRecordPacket(//NameRecordKey recordKey, 
          String name, int timeToLive,
          ValuesMap valuesMap,
          //ArrayList<String> values,
          HashSet<Integer> primaryNameServers, Set<Integer> activeNameServers, int primaryNameserverId) {
    this.type = Packet.PacketType.REPLICATE_RECORD;
    //this.recordKey = recordKey;
    this.name = name;
    this.timeToLive = timeToLive;
    //this.valuesList = values;
    this.valuesMap = valuesMap;
    this.primaryNameServers = primaryNameServers;
    this.activeNameServers = activeNameServers;
    this.primaryNameserverId = primaryNameserverId;
  }

//  /**
//   * ***********************************************************
//   * Constructs a new ReplicateRecordPacket from a NameRecord
//   *
//   * @param nameRecord NameRecord
//   ***********************************************************
//   */
//  public ReplicateRecordPacket(NameRecord nameRecord, int primaryNameserverId) {
//    this(//nameRecord.getRecordKey(),
//            nameRecord.getName(),
//            nameRecord.getTTL(),
//            new ValuesMap(nameRecord.getValuesMap()),
//            //new ArrayList<String>(nameRecord.getValuesList()),
//            nameRecord.getPrimaryNameservers(),
//            nameRecord.copyActiveNameServers(),
//            primaryNameserverId);
//  }

  /**
   * ***********************************************************
   * Constructs a new ReplicateRecordPacket from a JSONObject.
   *
   * @param json JSONObject representing this packet
   * @throws JSONException
   ***********************************************************
   */
  public ReplicateRecordPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.REPLICATE_RECORD) {
      Exception e = new Exception("ReplicateRecordPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
    }
    
    this.type = Packet.PacketType.REPLICATE_RECORD;
    //this.recordKey = NameRecordKey.valueOf((String) json.getString(RECORDKEY));
    this.name = json.getString(NAME);
    this.timeToLive = json.getInt(TIMETOLIVE);
    this.valuesMap = new ValuesMap(json.getJSONObject(VALUESMAP));
    //this.valuesList = JSONUtils.JSONArrayToArrayList(json.getJSONArray(VALUESLIST));
    this.primaryNameServers = (HashSet<Integer>) JSONUtils.JSONArrayToSetInteger(json.getJSONArray(PRIMARY_NAMESERVERS));
    this.activeNameServers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(SECONDARY_NAMESERVERS));
    this.primaryNameserverId = json.getInt(PRIMARY_NAMESERVER_ID);
  }

  /**
   * ***********************************************************
   * Converts ReplicateRecordPacket object to a JSONObject
   *
   * @return JSONObject that represents this packet
   * @throws JSONException
   ***********************************************************
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
   
    json.put(VALUESMAP, valuesMap.toJSONObject());
    //json.put(RECORDKEY, getRecordKey().getName());
    json.put(NAME, getName());
    json.put(TIMETOLIVE, getTimeToLive());
    //json.put(VALUESLIST, new JSONArray(getValuesList()));
    json.put(PRIMARY_NAMESERVERS, new JSONArray(getPrimaryNameServers()));
    json.put(SECONDARY_NAMESERVERS, new JSONArray(getActiveNameServers()));
    json.put(PRIMARY_NAMESERVER_ID, getPrimaryNameserverId());
    return json;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the timeToLive
   */
  public int getTimeToLive() {
    return timeToLive;
  }

  /**
   * @return the valuesMap
   */
  public ValuesMap getValuesMap() {
    return valuesMap;
  }

  /**
   * @return the primaryNameServers
   */
  public HashSet<Integer> getPrimaryNameServers() {
    return primaryNameServers;
  }

  /**
   * @return the activeNameServers
   */
  public Set<Integer> getActiveNameServers() {
    return activeNameServers;
  }

  /**
   * @return the primaryNameserverId
   */
  public int getPrimaryNameserverId() {
    return primaryNameserverId;
  }
}
