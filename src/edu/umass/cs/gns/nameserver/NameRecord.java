package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.packet.UpdateOperation;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.JSONUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 7/26/13
 * Time: 12:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class NameRecord {
    /**
     * Name (host/domain) *
     */
    private String name;
    /**
     * Map of values of this record *
     */
    private ValuesMap valuesMap;
    /**
     * Map of values from old active name servers *
     */
    private ValuesMap oldValuesMap;
    /**
     * TTL: time to live IN SECONDS*
     */
    private int timeToLive = 0; // 0 means TTL - 0 (no caching), -1 means the record will never expire if it gets into a cache

    /**
     * Set of primary nameservers *
     */
    private HashSet<Integer> primaryNameservers;
    /**
     * Set of active nameservers most recently computed.
     */
    private Set<Integer> activeNameservers;

    /**
     * Paxos instance ID of the new active name server set.
     */
    private String activePaxosID;

    /**
     * Paxos instance ID of the old active name server set
     */
    private String oldActivePaxosID;

    /**
     * Number of lookups *
     */
    private int totalLookupRequest;
    /**
     * Number of updates *
     */
    private int totalUpdateRequest;

    public NameRecord(String name) {
        this.name = name;
        //Initialize the entry in the map
        primaryNameservers = (HashSet<Integer>) HashFunction.getPrimaryReplicas(name);
        this.valuesMap = new ValuesMap();
        this.oldValuesMap = new ValuesMap();
        if (StartNameServer.debugMode) {
            GNS.getLogger().finer("Constructor Primaries: " + primaryNameservers);
        }

        this.activeNameservers = getInitialActives(primaryNameservers, StartNameServer.minReplica, name);
        if (StartNameServer.debugMode) {
            GNS.getLogger().finer(" Name Record INITIAL ACTIVES ARE: " + activeNameservers);
        }
        this.oldActivePaxosID = name + "-1"; // initialized uniformly among primaries
        this.activePaxosID = name + "-2";

        this.totalLookupRequest = 0;
        this.totalUpdateRequest = 0;
    }

    private final static String USER_KEYS = "nr_user_keys";
    private final static String OLDVALUESMAP = "nr_oldValuesMap";
    //
    public final static String NAME = MongoRecords.PRIMARYKEY;
    public final static String KEY = "nr_key"; // legacy use
    public final static String TIMETOLIVE = "nr_timeToLive";
    public final static String PRIMARY_NAMESERVERS = "nr_primary";
    public final static String ACTIVE_NAMESERVERS = "nr_active";
    public final static String ACTIVE_PAXOS_ID = "nr_activePaxosID";
    public final static String OLD_ACTIVE_PAXOS_ID = "nr_oldActivePaxosID";
    public final static String TOTALLOOKUPREQUEST = "nr_totalLookupRequest";
    public final static String TOTALUPDATEREQUEST = "nr_totalUpdateRequest";

    public NameRecord(JSONObject json) throws JSONException {
        this.valuesMap = new ValuesMap();
        // extract the user keys out of the json object
        for (String key : JSONUtils.JSONArrayToArrayList(json.getJSONArray(USER_KEYS))) {
            this.valuesMap.put(key, new QueryResultValue(JSONUtils.JSONArrayToArrayList(json.getJSONArray(key))));
        }
        this.oldValuesMap = new ValuesMap(json.getJSONObject(OLDVALUESMAP));

        this.name = json.getString(NAME);
        this.timeToLive = json.getInt(TIMETOLIVE);

        this.primaryNameservers = (HashSet<Integer>) JSONUtils.JSONArrayToSetInteger(json.getJSONArray(PRIMARY_NAMESERVERS));
        this.activeNameservers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(ACTIVE_NAMESERVERS));

        this.oldActivePaxosID = json.getString(OLD_ACTIVE_PAXOS_ID);
        if (!json.has(ACTIVE_PAXOS_ID)) {
            this.activePaxosID = null;
        } else {
            this.activePaxosID = json.getString(ACTIVE_PAXOS_ID);
        }
        this.totalLookupRequest = json.getInt(TOTALLOOKUPREQUEST);
        this.totalUpdateRequest = json.getInt(TOTALUPDATEREQUEST);
    }

    public NameRecord(String s, NameRecordKey nameRecordKey, ArrayList<String> values) {
        // TODO write this contstructor
    }

    public synchronized JSONObject toJSONObject() throws JSONException {
        JSONObject json = new JSONObject();
        // the reason we're putting the "user level" keys "inline" with all the other keys is
        // so that we can later access the metadata and the data using the same mechanism, namely a
        // lookup of the fields in the database. Alternatively, we could have separate storage and lookup
        // of data and metadata.
        valuesMap.addToJSONObject(json);
        json.put(USER_KEYS, new JSONArray(valuesMap.keySet()));

        json.put(OLDVALUESMAP, oldValuesMap.toJSONObject());
        //
        json.put(NAME, getName());
        json.put(TIMETOLIVE, timeToLive);
        json.put(PRIMARY_NAMESERVERS, new JSONArray(primaryNameservers));
        json.put(ACTIVE_NAMESERVERS, new JSONArray(activeNameservers));

        // new fields
        json.put(ACTIVE_PAXOS_ID, activePaxosID);
        json.put(OLD_ACTIVE_PAXOS_ID, oldActivePaxosID);


        json.put(TOTALLOOKUPREQUEST, totalLookupRequest);
        json.put(TOTALUPDATEREQUEST, totalUpdateRequest);

        return json;
    }

    private static Set<Integer> getInitialActives(Set<Integer> primaryNameservers, int count, String name) {
        // choose three actives which are different from primaries
        Set<Integer> newActives = new HashSet<Integer>();
        Random r = new Random(name.hashCode());

        for (int j = 0; j < count; j++) {
            while (true) {
                int id = r.nextInt(ConfigFileInfo.getNumberOfNameServers());
                //            int active = (j + id)% ConfigFileInfo.getNumberOfNameServers();
                if (!primaryNameservers.contains(id) && !newActives.contains(id)) {
                    GNS.getLogger().finer("ID " + id);
                    newActives.add(id);
                    break;
                }
            }
        }
        if (newActives.size() < count) {
            if (StartNameServer.debugMode) {
                GNS.getLogger().warning(" ERROR: initial actives < " + count + " Initial Actives: " + newActives);
            }
        }
        return newActives;
    }



    public HashSet<Integer> getPrimaryNameservers() {
        return primaryNameservers;
    }
    /**
     * @return the name
     */
    public synchronized String getName() {
        return name;
    }


    public synchronized ValuesMap getValuesMap() {
        return valuesMap;
    }

    public synchronized int getTTL() {
        return timeToLive;
    }

    public synchronized void setTTL(int ttl) {
        this.timeToLive = ttl;
    }

    public synchronized boolean containsKey(String key) {
        return valuesMap.containsKey(key);
    }

    public synchronized QueryResultValue get(String key) {
        return valuesMap.get(key);
    }

    public synchronized void put(String key, QueryResultValue value) {
        valuesMap.put(key, value);
    }

    /**
     * Returns a copy of the active name servers set.
     */
    public synchronized Set<Integer> copyActiveNameServers() { //synchronized
        Set<Integer> set = new HashSet<Integer>();
        for (int id : activeNameservers) {
            set.add(id);
        }
        return set;
    }

    /**
     * Returns true if the name record contains an active name server with the given id. False otherwise.
     *
     * @param id Active name server id
     */
    public synchronized boolean containsActiveNameServer(int id) {
        if (activeNameservers == null) {
//			if (StartNameServer.debugMode) GNRS.getLogger().fine("Active name servers is null.");
            return false;
        }
//		if (StartNameServer.debugMode) GNRS.getLogger().fine("Name = "+ name + " Active name servers = " + activeNameservers + " Primary name servers = " + primaryNameservers);
        return activeNameservers.contains(id);
    }

    /**
     * Implements the updating of values in the namerecord for a given key.
     *
     * Note: If the key doesn't exist the underlying calls will create it.
     *
     * That notwithstanding, code that calls this should still check for the key existing in the name record
     * so that we can return error values to the client for non-upsert situations.
     *
     * @param key
     * @param newValues - a list of the new values
     * @param oldValues - a list of the old values, can be null
     * @param operation
     * @return
     */
    public synchronized boolean updateValuesMap(String key, ArrayList<String> newValues, ArrayList<String> oldValues, UpdateOperation operation) {
        return UpdateOperation.updateValuesMap(valuesMap, key, newValues, oldValues, operation);
    }

    /**
     *
     * Increments the number of lookups by 1.
     */
    public synchronized void incrementLookupRequest() {
        totalLookupRequest += 1;
    }

    /**
     *
     * Increments the number of updates by 1.
     */
    public synchronized void incrementUpdateRequest() {
        totalUpdateRequest += 1;
    }


    /**
     * Returns a total count on the number of lookup at this nameserver.
     */
    public synchronized int getTotalReadFrequency() { //synchronized
        return totalLookupRequest;
    }

    /**
     * Returns a total count on the number of updates at this nameserver.
     */
    public synchronized int getTotalWriteFrequency() {
        return totalUpdateRequest;
    }


    /**
     * return paxos ID for new activeNameServers
     *
     * @return
     */
    public synchronized String getActivePaxosID() {
        return activePaxosID;
    }

    /**
     * ACTIVE: checks whether paxosID is current active Paxos/oldactive paxos/neither. .
     *
     * @param paxosID
     * @return
     */
    public synchronized int getPaxosStatus(String paxosID) {
//		if (StartNameServer.debugMode) GNRS.getLogger().fine("NAME RECORD: Active Paxos = " + activePaxosID + " Old Active Paxos: " + oldActivePaxosID);
        if (activePaxosID != null && activePaxosID.equals(paxosID)) {
            return 1;
        }
        if (oldActivePaxosID != null && oldActivePaxosID.equals(paxosID)) {
            return 2;
        }
        return 3;
    }


    /**
     * ACTIVE: handles paxos instance corresponding to the current set of actives has stopped.
     *
     * @param paxosID ID of the active set stopped
     */
    public synchronized void handleCurrentActiveStop(String paxosID) {

        if (this.activePaxosID.equals(paxosID)) {
            // current set of actives has stopped.
            // copy all fields to "oldActive" variables
            // initialize "active" variables to null
            this.oldActivePaxosID = paxosID;
            this.activePaxosID = null;
            this.activeNameservers = null;
            this.oldValuesMap = valuesMap;
            this.valuesMap = new ValuesMap();
//      this.oldValuesList = valuesList;
//      this.valuesList = null;
            if (StartNameServer.debugMode) {
                GNS.getLogger().fine(" Updated variables after current active stopped. ");
            }
        } else {
        }
        // this stops the old active.
        // backup current state.

    }

    /**
     * ACTIVE:
     * When new actives are started, initialize the necessary variables.
     *
     * @param actives current set of active name servers
     * @param paxosID paxosID of the current name servers
     * @param currentValue starting value of active name servers
     */
    public synchronized void handleNewActiveStart(Set<Integer> actives, String paxosID,
                                                  //QueryResultValue currentValue
                                                  ValuesMap currentValue) {
        this.activeNameservers = actives;
//        this.activeRunning = true;
        this.activePaxosID = paxosID;
        this.valuesMap = currentValue;
    }

    /**
     * Return
     *
     * @param oldPaxosID
     * @return
     */
    public synchronized ValuesMap getOldValues(String oldPaxosID) {
        if (oldPaxosID.equals(this.oldActivePaxosID)) {
            //return oldValuesList;
            return oldValuesMap;
        }
        return null;
    }

    /**
     *
     * Returns a String representation of this NameRecord.
     */
    @Override
    public synchronized String toString() {
        try {
            return toJSONObject().toString();
        } catch (JSONException e) {
            return "Error printing NameRecord: " + e;
        }
    }


}
