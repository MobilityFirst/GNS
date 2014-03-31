package edu.umass.cs.gns.nsdesign.recordmap;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.util.StatsInfo;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.ConsistentHashing;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.MovingAverage;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class implements the record which a replica controller stores for a name.
 * <p/>
 * <p/>
 * <b>How to use this class:</b>
 * <p/>
 * <i>1. ADD a new record to database:</i>
 * Use the constructor <code>public ReplicaControllerRecord(String name, boolean initialize)</code> to create
 * an new record. Next, insert this record into database by calling <code>addNameRecordPrimary</code> in
 * <code>NameServer</code>.
 * <p/>
 * <i>2. READ a record:</i>
 * First, read record from database and create an in-memory <code>ReplicaControllerRecord</code> object. Then,
 * use the getter methods in this class to read individual fields. To read record from database, use one of the
 * methods in <code>NameServer</code>. If you try to read a field which you have not read from database, it
 * will throw a <code>FieldNotFoundException</code>.
 * <p/>
 * Usually, a part of the code needs to read a few fields from the database. As database reads are not a
 * cheap operation, read all fields that are needed in executing a part of code in a single read from database.
 * You could also read the entire record instead of reading only the fields you need, but reading complete records
 * could be expensive for large records. Hence we avoid doing so.
 * <p/>
 * <i>3. WRITE/UPDATE/MODIFY a record:</i>
 * An update operation usually modifies a few fields in the record. To make all these updates with a single
 * database operation, we have implemented a set of update methods in this class, e.g., see method
 * <code>updateActiveNameServers</code>. The parameters of these methods are values of fields that are to be
 * updated. Depending on the type of update (e.g., test-and-update, increment), these methods call the
 * appropriate function in {@link edu.umass.cs.gns.nsdesign.recordmap.RecordMapInterface} to do the update.
 * If your update method needs to read fields in the record, make sure that these fields are already
 * read from the database.
 * <p/>
 * Some update operations need not be preceded by a read operation. In such cases, you can create an empty
 * <code>ReplicaControllerRecord</code> object by calling the constructor
 * <code>ReplicaControllerRecord(String name)</code>. This constructor initializes only the name field in the record,
 * and does not result in a database read operation. You can then directly call the update method on this object.
 * An example of such a method is <code>addReplicaSelectionVote</code>.
 * <p/>
 * If the existing update methods are not sufficient, write a new update method using the same pattern.
 * <p/>
 * <p/>
 * <i>3. Add a new field to the record:</i>
 * There are four steps:
 * (1) Define the field as static {@link edu.umass.cs.gns.database.ColumnField} object in this class.
 * (2) Add this field in the constructor <code>public ReplicaControllerRecord(String name, boolean initialize)</code>.
 * (3) Update the method <code>toJSONObject</code>.
 * (4) Create a getter method for this field.
 * <p/>
 * <p/>
 * <p/>
 * <b>Internal Design:</b> This class uses a generic <code>HashMap</code> to store fields that are currently in
 * memory. The keys and values of the hash map are the field names (string) and their values respectively.
 * Using a generic <code>HashMap</code>  has the advantage that we can store different types of objects in the
 * same hash map. While reading a field, we need type conversion to get the actual object.
 * <p/>
 * In designing this class, we first thought of defining a class field for every field in the record.
 * This design would allocate pointers for all fields in the record every time a <code>ReplicaControllerRecord</code>
 * object is created.  We chose the <code>HashMap</code> design for its efficiency, as it only creates those fields
 * which are necessary.
 * <p/>
 * <p/>
 * We provide a few static methods in the class for interacting with <code>BasicRecordMap.</code>.
 * These methods provide a wrapper around <code>BasicRecordMap</code> so that the rest of system
 * only needs to interact with the class <code>ReplicaControllerRecord</code> and not with
 * <code>BasicRecordMap</code>. Also, if we change <code>BasicRecordMap</code>, then only this class
 * needs to be modified, and not the code in other classes in the system.
 * <p/>
 * <p/>
 * <b>Thread safety:</b> This class is not thread-safe.
 * <p/>
 * <p/>
 * The design of this class is similar to the design of {@link edu.umass.cs.gns.nsdesign.recordmap.NameRecord} class.
 *
 * @see edu.umass.cs.gns.nsdesign.recordmap.NameRecord
 * @see edu.umass.cs.gns.nameserver.NameServer
 * @see edu.umass.cs.gns.nsdesign.recordmap.RecordMapInterface
 * @see edu.umass.cs.gns.exceptions.FieldNotFoundException
 */
public class ReplicaControllerRecord {

  public final static ColumnField NAME = new ColumnField("rcr_name", ColumnFieldType.STRING);
  public final static ColumnField PRIMARY_NAMESERVERS = new ColumnField("rcr_primary", ColumnFieldType.SET_INTEGER);
  public final static ColumnField ACTIVE_NAMESERVERS = new ColumnField("rcr_active", ColumnFieldType.SET_INTEGER);
  public final static ColumnField OLD_ACTIVE_NAMESERVERS = new ColumnField("rcr_oldactive", ColumnFieldType.SET_INTEGER);
  public final static ColumnField ACTIVE_NAMESERVERS_RUNNING = new ColumnField("rcr_activeRunning", ColumnFieldType.BOOLEAN);
  public final static ColumnField ACTIVE_VERSION = new ColumnField("rcr_activeVersion", ColumnFieldType.INTEGER);
  public final static ColumnField OLD_ACTIVE_VERSION = new ColumnField("rcr_oldActiveVersion", ColumnFieldType.INTEGER);
  public final static ColumnField MARKED_FOR_REMOVAL = new ColumnField("rcr_markedForRemoval", ColumnFieldType.INTEGER);
  public final static ColumnField VOTES_MAP = new ColumnField("rcr_votesMap", ColumnFieldType.VOTES_MAP);
  public final static ColumnField STATS_MAP = new ColumnField("rcr_statsMap", ColumnFieldType.STATS_MAP);
  public final static ColumnField PREV_TOTAL_READ = new ColumnField("rcr_prevTotalRead", ColumnFieldType.INTEGER);
  public final static ColumnField PREV_TOTAL_WRITE = new ColumnField("rcr_prevTotalWrite", ColumnFieldType.INTEGER);
  public final static ColumnField MOV_AVG_READ = new ColumnField("rcr_movAvgRead", ColumnFieldType.LIST_INTEGER);
  public final static ColumnField MOV_AVG_WRITE = new ColumnField("rcr_movAvgWrite", ColumnFieldType.LIST_INTEGER);
  public final static ColumnField KEEP_ALIVE_TIME = new ColumnField("rcr_keepAlive", ColumnFieldType.INTEGER);

  private HashMap<ColumnField, Object> hashMap = new HashMap<ColumnField, Object>();

  /**
   * Record map object which provides an interface to database
   */
  private BasicRecordMap replicaControllerDB;

  /********************************************
   * CONSTRUCTORS
   * ******************************************/
  /**
   * This method creates a new initialized ReplicaControllerRecord. by filling in all the fields.
   * If false, this constructor is the same as <code>public ReplicaControllerRecord(String name)</code>.
   */
  public ReplicaControllerRecord(BasicRecordMap replicaControllerDB, String name, boolean initialize) {

    hashMap = new HashMap<ColumnField, Object>();
    hashMap.put(NAME, name);

    if (initialize == false) {
      return;
    }
    hashMap.put(PRIMARY_NAMESERVERS, ConsistentHashing.getReplicaControllerSet(name));
    hashMap.put(ACTIVE_NAMESERVERS, ConsistentHashing.getReplicaControllerSet(name));
    hashMap.put(OLD_ACTIVE_NAMESERVERS, ConsistentHashing.getReplicaControllerSet(name));

    hashMap.put(ACTIVE_NAMESERVERS_RUNNING, true);

    hashMap.put(ACTIVE_VERSION, 1);
    hashMap.put(OLD_ACTIVE_VERSION, 0);

    hashMap.put(MARKED_FOR_REMOVAL, 0);

    hashMap.put(VOTES_MAP, new ConcurrentHashMap<Integer, Integer>());
    hashMap.put(STATS_MAP, new ConcurrentHashMap<Integer, StatsInfo>());

    hashMap.put(PREV_TOTAL_READ, 0);
    hashMap.put(PREV_TOTAL_WRITE, 0);
    hashMap.put(MOV_AVG_READ, new ArrayList<Integer>());
    hashMap.put(MOV_AVG_WRITE, new ArrayList<Integer>());

    hashMap.put(KEEP_ALIVE_TIME, 0);

    this.replicaControllerDB = replicaControllerDB;
  }

  /**
   * ONLY FOR RUNNING EXPERIMENTS!!
   * This method creates a new initialized ReplicaControllerRecord. by filling in all the fields.
   * If false, this constructor is the same as <code>public ReplicaControllerRecord(String name)</code>.
   */
  public ReplicaControllerRecord(BasicRecordMap replicaControllerDB, String name, Set<Integer> actives, boolean initialize) {
    this(replicaControllerDB, name, initialize);

    if (Config.experimentMode == false) {
      GNS.getLogger().severe("Exception Exception: wrong constructor being used.");
      throw new RuntimeException();
    }

    if (initialize == false) return;
    hashMap.put(ACTIVE_NAMESERVERS, actives);
    hashMap.put(OLD_ACTIVE_NAMESERVERS, actives);
  }

  /**
   * creates an empty ReplicaControllerRecord object
   *
   * @param name
   */
  public ReplicaControllerRecord(BasicRecordMap replicaControllerDB, String name) {
    hashMap = new HashMap<ColumnField, Object>();
    hashMap.put(NAME, name);
    this.replicaControllerDB = replicaControllerDB;
  }


  /**
   * Constructor used by the initialize values read from database
   *
   * @param allValues
   */
  public ReplicaControllerRecord(BasicRecordMap replicaControllerDB, HashMap<ColumnField, Object> allValues) {
    this.hashMap = allValues;
    this.replicaControllerDB = replicaControllerDB;
  }


  /**
   * Creates a new ReplicaControllerRecord from a JSONObject.
   *
   * @param json
   * @throws org.json.JSONException
   */
  public ReplicaControllerRecord(BasicRecordMap replicaControllerDB, JSONObject json) throws JSONException {
    hashMap = new HashMap<ColumnField, Object>();

    if (json.has(NAME.getName())) {
      hashMap.put(NAME, json.getString(NAME.getName()));
    }

    if (json.has(PRIMARY_NAMESERVERS.getName())) {
      hashMap.put(PRIMARY_NAMESERVERS, JSONUtils.getObject(PRIMARY_NAMESERVERS, json));
    }
    if (json.has(ACTIVE_NAMESERVERS.getName())) {
      hashMap.put(ACTIVE_NAMESERVERS, JSONUtils.getObject(ACTIVE_NAMESERVERS, json));
    }
    if (json.has(OLD_ACTIVE_NAMESERVERS.getName())) {
      hashMap.put(OLD_ACTIVE_NAMESERVERS, JSONUtils.getObject(OLD_ACTIVE_NAMESERVERS, json));
    }

    if (json.has(ACTIVE_NAMESERVERS_RUNNING.getName())) {
      hashMap.put(ACTIVE_NAMESERVERS_RUNNING, JSONUtils.getObject(ACTIVE_NAMESERVERS_RUNNING, json));
    }

    if (json.has(ACTIVE_VERSION.getName())) {
      hashMap.put(ACTIVE_VERSION, JSONUtils.getObject(ACTIVE_VERSION, json));
    }
    if (json.has(OLD_ACTIVE_VERSION.getName())) {
      hashMap.put(OLD_ACTIVE_VERSION, JSONUtils.getObject(OLD_ACTIVE_VERSION, json));
    }

    if (json.has(MARKED_FOR_REMOVAL.getName())) {
      hashMap.put(MARKED_FOR_REMOVAL, JSONUtils.getObject(MARKED_FOR_REMOVAL, json));
    }

    if (json.has(VOTES_MAP.getName())) {
      hashMap.put(VOTES_MAP, JSONUtils.getObject(VOTES_MAP, json));
    }
    if (json.has(STATS_MAP.getName())) {
      hashMap.put(STATS_MAP, JSONUtils.getObject(STATS_MAP, json));
    }

    if (json.has(PREV_TOTAL_READ.getName())) {
      hashMap.put(PREV_TOTAL_READ, JSONUtils.getObject(PREV_TOTAL_READ, json));
    }
    if (json.has(PREV_TOTAL_WRITE.getName())) {
      hashMap.put(PREV_TOTAL_WRITE, JSONUtils.getObject(PREV_TOTAL_WRITE, json));
    }
    if (json.has(MOV_AVG_READ.getName())) {
      hashMap.put(MOV_AVG_READ, JSONUtils.getObject(MOV_AVG_READ, json));
    }
    if (json.has(MOV_AVG_WRITE.getName())) {
      hashMap.put(MOV_AVG_WRITE, JSONUtils.getObject(MOV_AVG_WRITE, json));
    }

    if (json.has(KEEP_ALIVE_TIME.getName())) {
      hashMap.put(KEEP_ALIVE_TIME, JSONUtils.getObject(KEEP_ALIVE_TIME, json));
    }

    this.replicaControllerDB = replicaControllerDB;
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject();
//    GNS.getLogger().fine("hash map --> " + hashMap);
    for (ColumnField f : hashMap.keySet()) {

      JSONUtils.putFieldInJsonObject(f, hashMap.get(f), jsonObject);
    }
    return jsonObject;
  }

  @Override
  public String toString() {
    try {
      return toJSONObject().toString();
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return null;
  }

  /********************************************
   * GETTER methods for each ColumnField in replica controller record
   * ******************************************/
  /**
   * @return the name
   */
  public String getName() throws FieldNotFoundException {
    if (hashMap.containsKey(NAME)) {
      return (String) hashMap.get(NAME);
    }
    throw new FieldNotFoundException(NAME);
  }

  /**
   * Returns the PrimaryNameservers.
   *
   * @return primaryNameservers as a set of Integers
   */
  public HashSet<Integer> getPrimaryNameservers() throws FieldNotFoundException {
    if (hashMap.containsKey(PRIMARY_NAMESERVERS)) {
      return (HashSet<Integer>) hashMap.get(PRIMARY_NAMESERVERS);
    }
    throw new FieldNotFoundException(PRIMARY_NAMESERVERS);
  }

  /**
   * @return the activeNameservers
   */
  public Set<Integer> getActiveNameservers() throws FieldNotFoundException {
    if (hashMap.containsKey(ACTIVE_NAMESERVERS)) {
      return (Set<Integer>) hashMap.get(ACTIVE_NAMESERVERS);
    }
    throw new FieldNotFoundException(ACTIVE_NAMESERVERS);
  }

  /**
   * return the set of active name servers in the network.
   *
   * @return
   */
  public Set<Integer> getOldActiveNameservers() throws FieldNotFoundException {
    if (hashMap.containsKey(OLD_ACTIVE_NAMESERVERS)) {
      return (Set<Integer>) hashMap.get(OLD_ACTIVE_NAMESERVERS);
    }
    throw new FieldNotFoundException(OLD_ACTIVE_NAMESERVERS);
  }

  /**
   * are the activeNameServers running?
   *
   * @return
   */
  public boolean isActiveRunning() throws FieldNotFoundException {
    if (hashMap.containsKey(ACTIVE_NAMESERVERS_RUNNING)) {
      return (Boolean) hashMap.get(ACTIVE_NAMESERVERS_RUNNING);
    }
    throw new FieldNotFoundException(ACTIVE_NAMESERVERS_RUNNING);
  }

  /**
   * return version number for new activeNameServers
   *
   * @return
   */
  public int getActiveVersion() throws FieldNotFoundException {
    if (hashMap.containsKey(ACTIVE_VERSION)) {
      return (Integer) hashMap.get(ACTIVE_VERSION);
    }
    throw new FieldNotFoundException(ACTIVE_VERSION);
  }

  /**
   * return version number for the oldActiveNameSevers
   *
   * @return
   */
  public int getOldActiveVersion() throws FieldNotFoundException {
    if (hashMap.containsKey(OLD_ACTIVE_VERSION)) {
      return (Integer) hashMap.get(OLD_ACTIVE_VERSION);
    }
    throw new FieldNotFoundException(OLD_ACTIVE_VERSION);
  }

  public int getMarkedForRemoval() throws FieldNotFoundException {
    if (hashMap.containsKey(MARKED_FOR_REMOVAL)) {
      return (Integer) hashMap.get(MARKED_FOR_REMOVAL);
    }
    throw new FieldNotFoundException(MARKED_FOR_REMOVAL);
  }

  /**
   * @return the nameServerVotesMap
   */
  public ConcurrentMap<Integer, Integer> getNameServerVotesMap() throws FieldNotFoundException {
    if (hashMap.containsKey(VOTES_MAP)) {
      return (ConcurrentMap<Integer, Integer>) hashMap.get(VOTES_MAP);
    }
    throw new FieldNotFoundException(VOTES_MAP);
  }

  /**
   * @return the nameServerStatsMap
   */
  public ConcurrentMap<Integer, StatsInfo> getNameServerStatsMap() throws FieldNotFoundException {
    if (hashMap.containsKey(STATS_MAP)) {
      return (ConcurrentMap<Integer, StatsInfo>) hashMap.get(STATS_MAP);
    }
    throw new FieldNotFoundException(STATS_MAP);
  }

  /**
   * @return the previousAggregateReadFrequency
   */
  public int getPreviousAggregateReadFrequency() throws FieldNotFoundException {
    if (hashMap.containsKey(PREV_TOTAL_READ)) {
      return (Integer) hashMap.get(PREV_TOTAL_READ);
    }
    throw new FieldNotFoundException(PREV_TOTAL_READ);
  }

  /**
   * @return the previousAggregateWriteFrequency
   */
  public int getPreviousAggregateWriteFrequency() throws FieldNotFoundException {
    if (hashMap.containsKey(PREV_TOTAL_WRITE)) {
      return (Integer) hashMap.get(PREV_TOTAL_WRITE);
    }
    throw new FieldNotFoundException(PREV_TOTAL_WRITE);
  }

  /**
   * @return the movingAvgAggregateLookupFrequency
   */
  public ArrayList<Integer> getMovingAvgAggregateLookupFrequency() throws FieldNotFoundException {
    if (hashMap.containsKey(MOV_AVG_READ)) {
      return (ArrayList<Integer>) hashMap.get(MOV_AVG_READ);
    }
    throw new FieldNotFoundException(MOV_AVG_READ);
  }

  /**
   * @return the movingAvgAggregateUpdateFrequency
   */
  public ArrayList<Integer> getMovingAvgAggregateUpdateFrequency() throws FieldNotFoundException {
    if (hashMap.containsKey(MOV_AVG_WRITE)) {
      return (ArrayList<Integer>) hashMap.get(MOV_AVG_WRITE);
    }
    throw new FieldNotFoundException(MOV_AVG_WRITE);
  }

  /**
   * return the keep alive time value stored in record
   *
   * @return
   */
  public int getKeepAliveTime() throws FieldNotFoundException {
    if (hashMap.containsKey(KEEP_ALIVE_TIME)) {
      return (Integer) hashMap.get(KEEP_ALIVE_TIME);
    }
    throw new FieldNotFoundException(KEEP_ALIVE_TIME);
  }

  /**
   * *****************************************
   * READ methods: these methods only read one or more fields. they use the above GETTER methods to access the values of fields.
   * *****************************************
   */

  /**
   * whether the flag is set of not.
   *
   * @return
   */
  public boolean isMarkedForRemoval() throws FieldNotFoundException {
    // todo make marked for removal a boolean instead of int
    return getMarkedForRemoval() > 0;
  }

  /**
   * Returns true if the name record contains a primary name server with the given id. False otherwise.
   *
   * @param id Primary name server id
   */
  public boolean containsPrimaryNameserver(int id) throws FieldNotFoundException {
    Set<Integer> primaries = getPrimaryNameservers();
    if (primaries != null) {
      return primaries.contains(id);
    }
    return false;
  }

  /**
   * Returns a set of highest voted name servers ids.
   *
   * @param numReplica Number of name servers to be selected.
   */
  public Set<Integer> getHighestVotedReplicaID(int numReplica) throws FieldNotFoundException { //
    Set<Integer> replicas = new HashSet<Integer>();

    for (int i = 1; i <= numReplica; i++) {
      int highestVotes = -1;
      int highestVotedReplicaID = -1;

      for (Map.Entry<Integer, Integer> entry : getNameServerVotesMap().entrySet()) {
        int nameServerId = entry.getKey();
        int votes = entry.getValue();
        //Skip name server that are unreachable
        // from main branch 269
        if (ConfigFileInfo.getPingLatency(nameServerId) == ConfigFileInfo.INVALID_PING_LATENCY) {
          continue;
        }
        if (!replicas.contains(nameServerId)
                && votes > highestVotes) {
          highestVotes = votes;
          highestVotedReplicaID = nameServerId;
        }
      }
      //Checks whether a new replica was available to be added
      if (highestVotedReplicaID != -1) {
        replicas.add(highestVotedReplicaID);
      } else {
        break;
      }

      if (replicas.size() == getNameServerVotesMap().size()) {
        break;
      }
    }
    return replicas;
  }


  /**
   * *****************************************
   * WRITE methods: these methods write one or more fields. they may read values of some fields using the above GETTER methods.
   * *****************************************
   */
  private static ArrayList<ColumnField> setMarkedForRemoval = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getSetMarkedForRemoval() {
    synchronized (setMarkedForRemoval) {
      if (setMarkedForRemoval.size() > 0) {
        return setMarkedForRemoval;
      }
      setMarkedForRemoval.add(MARKED_FOR_REMOVAL);
      return setMarkedForRemoval;
    }
  }

  /**
   * set marked for removal : 0 --> 1. 1 means record is in the process of being removed.
   * On update, it checks if active name servers are running, i. e., a group change is not happening at same time.
   * If group change is happening simultaneously, update is not applied.
   */
  public void setMarkedForRemoval() throws FieldNotFoundException {
    int markedForRemoval = getMarkedForRemoval();
    GNS.getLogger().fine("Marked for removal value: " + markedForRemoval);
    if (markedForRemoval == 0) {
      ArrayList<ColumnField> fields = getSetMarkedForRemoval();

      ArrayList<Object> values = new ArrayList<Object>();
      values.add(1);

      replicaControllerDB.updateConditional(getName(), NAME, ACTIVE_NAMESERVERS_RUNNING, true, fields, values,
              null,null,null);
      replicaControllerDB.update(getName(), NAME, fields, values);

      // since this is a conditional update, re-read record to check if update was actually applied.
      // update hash map accordingly.
      try {
        ReplicaControllerRecord replicaControllerRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
                replicaControllerDB, getName(), MARKED_FOR_REMOVAL);
        if(replicaControllerRecord.isMarkedForRemoval()) {
          hashMap.put(MARKED_FOR_REMOVAL, 1);
        } else{
          hashMap.put(MARKED_FOR_REMOVAL, 0);
        }
      } catch (RecordNotFoundException e) {
        e.printStackTrace();
        return;
      }
    }
  }

  private static ArrayList<ColumnField> updateActiveNameServerFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getUpdateActiveNameServerFields() {
    synchronized (updateActiveNameServerFields) {
      if (updateActiveNameServerFields.size() > 0) {
        return updateActiveNameServerFields;
      }
      updateActiveNameServerFields.add(ACTIVE_NAMESERVERS_RUNNING);
      updateActiveNameServerFields.add(OLD_ACTIVE_NAMESERVERS);
      updateActiveNameServerFields.add(ACTIVE_NAMESERVERS);
      updateActiveNameServerFields.add(OLD_ACTIVE_VERSION);
      updateActiveNameServerFields.add(ACTIVE_VERSION);
      return updateActiveNameServerFields;
    }
  }

  /**
   * Makes current active name servers old active name servers, and sets the new active name servers to true.
   *
   * @param newActiveNameServers
   * @param newActiveVersion
   */
  public void updateActiveNameServers(Set<Integer> newActiveNameServers, int newActiveVersion) throws FieldNotFoundException {

//    boolean activeRunning = isActiveRunning();
    Set<Integer> actives = getActiveNameservers();
    int activeVersion = getActiveVersion();

    ArrayList<ColumnField> updateFields = getUpdateActiveNameServerFields();

    ArrayList<Object> updateValues = new ArrayList<Object>();

//    updateValues.add(activeRunning);
    updateValues.add(false);
    updateValues.add(actives);
    updateValues.add(newActiveNameServers);
    updateValues.add(activeVersion);
    updateValues.add(newActiveVersion);

    replicaControllerDB.update(getName(), NAME, updateFields, updateValues);

//    hashMap.put(OLD_ACTIVE_NAMESERVERS_RUNNING, activeRunning);
    hashMap.put(ACTIVE_NAMESERVERS_RUNNING, false);
    hashMap.put(OLD_ACTIVE_NAMESERVERS, actives);
    hashMap.put(ACTIVE_NAMESERVERS, newActiveNameServers);
    hashMap.put(OLD_ACTIVE_VERSION, activeVersion);
    hashMap.put(ACTIVE_VERSION, newActiveVersion);
  }

  private static ArrayList<ColumnField> setNewActiveRunningFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getSetNewActiveRunningFields() {
    synchronized (setNewActiveRunningFields) {
      if (setNewActiveRunningFields.size() > 0) {
        return setNewActiveRunningFields;
      }
      setNewActiveRunningFields.add(ACTIVE_NAMESERVERS_RUNNING);
//      setNewActiveRunningFields.add(OLD_ACTIVE_NAMESERVERS_RUNNING);
      return setNewActiveRunningFields;
    }
  }

  /**
   * Set new active running.
   *
   * @param newActiveID
   * @return
   */
  public boolean setNewActiveRunning(int newActiveID) throws FieldNotFoundException {
    if (newActiveID == this.getActiveVersion()) {

      ArrayList<ColumnField> updateFields = getSetNewActiveRunningFields();

      ArrayList<Object> values = new ArrayList<Object>();
      values.add(true);
      replicaControllerDB.update(getName(), NAME, updateFields, values);
      hashMap.put(ACTIVE_NAMESERVERS_RUNNING, true);
      return true;
    }
    return false;
  }


  /**
   * Returns the total number of lookup request across all active name servers
   *
   * @return
   */
  public double[] recomputeAverageReadWriteRate() throws FieldNotFoundException {
    int previousTotalReads = getPreviousAggregateReadFrequency();
    int previousTotalWrites = getPreviousAggregateWriteFrequency();
    MovingAverage lookups = new MovingAverage(getMovingAvgAggregateLookupFrequency(), Config.movingAverageWindowSize);
    MovingAverage updates = new MovingAverage(getMovingAvgAggregateUpdateFrequency(), Config.movingAverageWindowSize);

    lookups.add(previousTotalReads);
    updates.add(previousTotalWrites);

    double[] readWriteRate = new double[2];
    readWriteRate[0] = lookups.getAverage();
    readWriteRate[1] = updates.getAverage();


    ArrayList<ColumnField> updateFields = new ArrayList<ColumnField>();
    updateFields.add(PREV_TOTAL_READ);
    updateFields.add(PREV_TOTAL_WRITE);
    updateFields.add(MOV_AVG_READ);
    updateFields.add(MOV_AVG_WRITE);

    ArrayList<Object> values = new ArrayList<Object>();
    values.add(0);
    values.add(0);
    values.add(lookups.toArrayList());
    values.add(updates.toArrayList());

    replicaControllerDB.update(getName(), NAME, updateFields, values);

    hashMap.put(PREV_TOTAL_READ, 0);
    hashMap.put(PREV_TOTAL_WRITE, 0);
    hashMap.put(MOV_AVG_READ, lookups.toArrayList());
    hashMap.put(MOV_AVG_WRITE, updates.toArrayList());
    return readWriteRate;

  }

  /**
   * Adds vote to the name server for replica selection.
   * and increment lookup count and update count.
   *
   * @param id Name server id receiving the vote
   */
  public void addReplicaSelectionVote(int id, int vote, int update) throws FieldNotFoundException { //
    replicaControllerDB.increment(getName(),
            ColumnField.keys(PREV_TOTAL_READ, PREV_TOTAL_WRITE),
            //incrementFields,
            ColumnField.values(vote, update),
            //incrementValues,
            VOTES_MAP,
            ColumnField.keys(new ColumnField(Integer.toString(id), ColumnFieldType.INTEGER)),
            //votesMapKeys,
            ColumnField.values(vote) //votesMapValues
    );
  }

  /**
   * @param id
   * @param readFrequency
   * @param writeFrequency
   */
  public void addNameServerStats(int id, int readFrequency, int writeFrequency) throws FieldNotFoundException {
    ConcurrentMap<Integer, StatsInfo> statsMap = getNameServerStatsMap();

    if (statsMap != null) {
      statsMap.put(id, new StatsInfo(readFrequency, writeFrequency));
      ArrayList<ColumnField> updateFields = new ArrayList<ColumnField>();
      updateFields.add(STATS_MAP);

      ArrayList<Object> values = new ArrayList<Object>();
      values.add(statsMap);

      replicaControllerDB.update(getName(), NAME, updateFields, values);
      // StatsMap is already updated in hashMap.
    }
  }

  /********************************************
   * SETTER methods, these methods write to database one ColumnField in the name record.
   * ******************************************/
  /**
   * @param activeNameservers the activeNameservers to set
   */
  public void setActiveNameservers(Set<Integer> activeNameservers) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param oldActiveNameservers the oldActiveNameservers to set
   */
  public void setOldActiveNameservers(Set<Integer> oldActiveNameservers) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param oldActiveRunning the oldActiveRunning to set
   */
  public void setOldActiveRunning(boolean oldActiveRunning) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param activeRunning the activeRunning to set
   */
  public void setActiveRunning(boolean activeRunning) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param oldActiveVersion the oldActiveVersion to set
   */
  public void setOldActiveVersion(String oldActiveVersion) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param activeVersion the activeVersion to set
   */
  public void setActiveVersion(String activeVersion) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param previousAggregateReadFrequency the previousAggregateReadFrequency to set
   */
  public void setPreviousAggregateReadFrequency(int previousAggregateReadFrequency) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param previousAggregateWriteFrequency the previousAggregateWriteFrequency to set
   */
  public void setPreviousAggregateWriteFrequency(int previousAggregateWriteFrequency) {
    GNS.getLogger().severe("Not implemented yet because is is not used!");
    throw new UnsupportedOperationException();
  }

  /**
   * @param keepAliveTime set keep alive time
   */
  public void setKeepAliveTime(long keepAliveTime) throws FieldNotFoundException {
    ArrayList<ColumnField> fields = new ArrayList<ColumnField>();
    fields.add(KEEP_ALIVE_TIME);

    ArrayList<Object> values = new ArrayList<Object>();
    values.add(keepAliveTime);

    replicaControllerDB.update(getName(), NAME, fields, values);

  }


  /**
   BEGIN: static methods for reading/writing to database and iterating over records
   */


  /**
   * Read the complete ReplicaControllerRecord from database
   *
   * @param replicaControllerDB
   * @param name
   * @return
   */
  public static ReplicaControllerRecord getNameRecordPrimary(BasicRecordMap replicaControllerDB, String name)
          throws RecordNotFoundException {
    return replicaControllerDB.getNameRecordPrimary(name);
  }

  public static ReplicaControllerRecord getNameRecordPrimaryMultiField(BasicRecordMap replicaControllerDB,
                                                                       String name, ColumnField... fields)
          throws RecordNotFoundException {
    return getNameRecordPrimaryMultiField(replicaControllerDB, name, new ArrayList<ColumnField>(Arrays.asList(fields)));
  }

  /**
   * Read name record with select fields
   *
   * @param name
   * @param fields
   * @return
   * @throws edu.umass.cs.gns.exceptions.RecordNotFoundException
   */
  public static ReplicaControllerRecord getNameRecordPrimaryMultiField(BasicRecordMap replicaControllerDB, String name,
                                                                       ArrayList<ColumnField> fields)
          throws RecordNotFoundException {
    return new ReplicaControllerRecord(replicaControllerDB, replicaControllerDB.lookup(name, ReplicaControllerRecord.NAME, fields));
  }

  /**
   * Add this record to database
   *
   * @param record
   */
  public static void addNameRecordPrimary(BasicRecordMap replicaControllerDB, ReplicaControllerRecord record)
          throws RecordExistsException {
    replicaControllerDB.addNameRecordPrimary(record);
  }

  /**
   * Remove a ReplicaControllerRecord with given name from database
   *
   * @param name
   */
  public static void removeNameRecordPrimary(BasicRecordMap replicaControllerDB, String name) {
    replicaControllerDB.removeNameRecord(name);
  }

  /**
   * Replace the ReplicaControllerRecord in DB with this copy of ReplicaControllerRecord
   *
   * @param record
   */
  public static void updateNameRecordPrimary(BasicRecordMap replicaControllerDB, ReplicaControllerRecord record) {
    replicaControllerDB.updateNameRecordPrimary(record);
  }

  public static BasicRecordCursor getAllPrimaryRowsIterator(BasicRecordMap replicaControllerDB) {
    return replicaControllerDB.getAllRowsIterator();
  }

  //  the nuclear option
  public static void resetDB(BasicRecordMap replicaControllerDB) {
    replicaControllerDB.reset();
  }

  /**
   * END: static methods for reading/writing to database and iterating over records
   */


  // test code
  public static void main(String[] args) throws FieldNotFoundException, Exception {
    test();
    //System.exit(0);
  }

  // make this query:
  // http://127.0.0.1:8080/GNS/registerAccount?name=sally&publickey=dummy3
  private static void test() throws FieldNotFoundException, Exception {
    Config.movingAverageWindowSize = 10;
    int nodeID = 4;
    ConfigFileInfo.readHostInfo("ns1", nodeID);
    ConsistentHashing.initialize(GNS.numPrimaryReplicas, ConfigFileInfo.getNumberOfNameServers());
    // fixme set parameter to non-null in constructor
    BasicRecordMap replicaController = new MongoRecordMap(null, MongoRecords.DBREPLICACONTROLLER);
    replicaController.reset();
    ReplicaControllerRecord record = new ReplicaControllerRecord(replicaController, "1A434C0DAA0B17E48ABD4B59C632CF13501C7D24");
    record.addReplicaSelectionVote(1, 5, 4);
    record.addReplicaSelectionVote(1, 1, 4);
    record.addReplicaSelectionVote(2, 2, 4);
    record.addReplicaSelectionVote(3, 3, 4);
    record.addReplicaSelectionVote(4, 4, 4);
    record.addNameServerStats(1, 50, 75);
    record.addNameServerStats(2, 50, 75);
    System.out.println(record.toJSONObject().toString());
    try {
      replicaController.addNameRecordPrimary(record);
    } catch (RecordExistsException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    // create the lazy record
    record = new ReplicaControllerRecord(replicaController, "1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", true);
    System.out.println("PRIMARY NS: " + record.getPrimaryNameservers());
    System.out.println("CONTAINS ACTIVE NS: " + record.containsPrimaryNameserver(12));
    record.addNameServerStats(10, 50, 75);
    System.out.println("READ STATS: " + record.recomputeAverageReadWriteRate());

    record.addReplicaSelectionVote(11, 5, 0);
    record.addReplicaSelectionVote(11, 1, 0);
    record.addReplicaSelectionVote(21, 2, 0);
    record.addReplicaSelectionVote(13, 3, 0);
    record.addReplicaSelectionVote(14, 4, 0);
    record.addNameServerStats(11, 50, 75);
    record.addNameServerStats(12, 50, 75);
    System.out.println("3 HIGHEST VOTES: " + record.getHighestVotedReplicaID(3));
//    record.updateMovingAvgAggregateLookupFrequency(10);
//    record.updateMovingAvgAggregateLookupFrequency(30);
//    record.updateMovingAvgAggregateLookupFrequency(50);
    System.out.println("MOVING AG READ: " + record.getMovingAvgAggregateLookupFrequency());

    MongoRecords instance = MongoRecords.getInstance();
    instance.printAllEntries(MongoRecords.DBREPLICACONTROLLER);

    try {
      record = replicaController.getNameRecordPrimary("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24");
    } catch (RecordNotFoundException e) {

      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    System.out.println(record.toJSONObject().toString());

  }
}
