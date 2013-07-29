package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.StatsInfo;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.MovingAverage;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 7/26/13
 * Time: 12:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class ReplicaControllerRecord {

    /**
     * Name (host/domain) *
     */
    private String name;

    /**
     * Set of primary nameservers *
     */
    private HashSet<Integer> primaryNameservers;


    /**
     * Set of active nameservers most recently computed.
     */
    private Set<Integer> activeNameservers;
    /**
     * Previous set of active nameservers
     */
    private Set<Integer> oldActiveNameservers;
    /**
     * Whether previous set of active name server is active
     */
    private boolean oldActiveRunning = false;
    /**
     * Whether current set of active name servers is stopped.
     */
    private boolean activeRunning = false;
    /**
     * Paxos instance ID of the old active name server set
     */
    private String oldActivePaxosID;
    /**
     * Paxos instance ID of the new active name server set.
     */
    private String activePaxosID;
    /**
     * At primary name servers, this field means that the name record is going to be removed once current set of active is deleted.
     */
    private boolean markedForRemoval = false;
    /**
     * Read write rates reported from name server.
     */
    private ConcurrentMap<Integer, StatsInfo> nameServerStatsMap;

    private MovingAverage movingAvgAggregateLookupFrequency = null;
    private MovingAverage movingAvgAggregateUpdateFrequency = null;

    private int totalAggregateReadFrequency;
    private int totalAggregateWriteFrequency;
    private int previousAggregateReadFrequency;
    private int previousAggregateWriteFrequency;
    /**
     * List of name server voted as a replica for this record. <Key = NameserverID, Value=#Votes}
     */
    private ConcurrentMap<Integer, Integer> nameServerVotesMap;


    public enum ACTIVE_STATE {
        ACTIVE_RUNNING,
        OLD_ACTIVE_RUNNING,
        NO_ACTIVE_RUNNING,
        BOTH_ACTIVE_RUNNING_ERROR
    };
    /**
     * This method creates a new namerecordprimary.
     */
    public ReplicaControllerRecord(String name) {
        this.name = name;
        //Initialize the entry in the map
        primaryNameservers = (HashSet<Integer>) HashFunction.getPrimaryReplicas(name);
//        if (primaryNameservers.contains(NameServer.nodeID) == false)
        if (StartNameServer.debugMode) {
            GNS.getLogger().finer("Constructor Primaries: " + primaryNameservers);
        }

        this.nameServerStatsMap = new ConcurrentHashMap<Integer, StatsInfo>(10, 0.75f, 3);

        this.oldActiveNameservers = new HashSet<Integer>(primaryNameservers);
        this.activeNameservers = getInitialActives(primaryNameservers, StartNameServer.minReplica, name);
        if (StartNameServer.debugMode) {
            GNS.getLogger().finer(" Name Record INITIAL ACTIVES ARE: " + activeNameservers);
        }
        this.oldActivePaxosID = name + "-1"; // initialized uniformly among primaries
        this.activePaxosID = name + "-2";

        this.totalAggregateReadFrequency = 0;
        this.totalAggregateWriteFrequency = 0;
        this.previousAggregateReadFrequency = 0;
        this.previousAggregateWriteFrequency = 0;
        this.nameServerVotesMap = new ConcurrentHashMap<Integer, Integer>(10, 0.75f, 3);
        this.movingAvgAggregateLookupFrequency = new MovingAverage(StartNameServer.movingAverageWindowSize);
        this.movingAvgAggregateUpdateFrequency = new MovingAverage(StartNameServer.movingAverageWindowSize);

    }


    public final static String NAME = MongoRecords.PRIMARYKEY;
    public final static String PRIMARY_NAMESERVERS = "nr_primary";
    public final static String ACTIVE_NAMESERVERS = "nr_active";
    public final static String OLD_ACTIVE_NAMESERVERS = "nr_oldactive";
    public final static String ACTIVE_NAMESERVERS_RUNNING = "nr_activeRunning";
    public final static String OLD_ACTIVE_NAMESERVERS_RUNNING = "nr_oldActiveRunning";
    public final static String ACTIVE_PAXOS_ID = "nr_activePaxosID";
    public final static String OLD_ACTIVE_PAXOS_ID = "nr_oldActivePaxosID";
    public final static String MARKED_FOR_REMOVAL = "nr_markedForRemoval";
    public final static String NAMESERVER_VOTES_MAP = "nr_nameserverVotesMap";
    public final static String NAMESERVERSTATSMAP = "nr_nameServerStatsMap";
    public final static String TOTALAGGREGATEREADFREQUENCY = "nr_totalAggregateReadFrequency";
    public final static String TOTALAGGREGATEWRITEFREQUENCY = "nr_totalAggregateWriteFrequency";
    public final static String PREVIOUSAGGREAGATEREADFREQUENCY = "nr_previousAggregateReadFrequency";
    public final static String PREVIOUSAGGREAGATEWRITEFREQUENCY = "nr_previousAggregateWriteFrequency";
    public final static String MOVINGAGGREGATELOOKUPFREQUENCY = "nr_movingAvgAggregateLookupFrequency";
    public final static String MOVINGAGGREGATEUPDATEFREQUENCY = "nr_movingAvgAggregateUpdateFrequency";

    public ReplicaControllerRecord(JSONObject json) throws JSONException {

        this.name = json.getString(NAME);

        this.primaryNameservers = (HashSet<Integer>) JSONUtils.JSONArrayToSetInteger(json.getJSONArray(PRIMARY_NAMESERVERS));
        this.activeNameservers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(ACTIVE_NAMESERVERS));

        // New fields
        this.oldActiveNameservers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(OLD_ACTIVE_NAMESERVERS));
        this.oldActiveRunning = json.getBoolean(OLD_ACTIVE_NAMESERVERS_RUNNING);
        this.activeRunning = json.getBoolean(ACTIVE_NAMESERVERS_RUNNING);
        this.oldActivePaxosID = json.getString(OLD_ACTIVE_PAXOS_ID);
        if (!json.has(ACTIVE_PAXOS_ID)) {
            this.activePaxosID = null;
        } else {
            this.activePaxosID = json.getString(ACTIVE_PAXOS_ID);
        }
        this.markedForRemoval = json.getBoolean(MARKED_FOR_REMOVAL);

        this.nameServerVotesMap = toIntegerMap(json.getJSONObject(NAMESERVER_VOTES_MAP));
        this.nameServerStatsMap = toStatsMap(json.getJSONObject(NAMESERVERSTATSMAP));

        this.totalAggregateReadFrequency = json.getInt(TOTALAGGREGATEREADFREQUENCY);
        this.totalAggregateWriteFrequency = json.getInt(TOTALAGGREGATEWRITEFREQUENCY);
        this.previousAggregateReadFrequency = json.getInt(PREVIOUSAGGREAGATEREADFREQUENCY);
        this.previousAggregateWriteFrequency = json.getInt(PREVIOUSAGGREAGATEWRITEFREQUENCY);


        if (json.has(MOVINGAGGREGATELOOKUPFREQUENCY)) {
            this.movingAvgAggregateLookupFrequency = new MovingAverage(json.getJSONArray(MOVINGAGGREGATELOOKUPFREQUENCY), StartNameServer.movingAverageWindowSize);
        }
        if (json.has(MOVINGAGGREGATEUPDATEFREQUENCY)) {
            this.movingAvgAggregateUpdateFrequency = new MovingAverage(json.getJSONArray(MOVINGAGGREGATEUPDATEFREQUENCY), StartNameServer.movingAverageWindowSize);
        }
    }

    public synchronized JSONObject toJSONObject() throws JSONException {
        JSONObject json = new JSONObject();
        // the reason we're putting the "user level" keys "inline" with all the other keys is
        // so that we can later access the metadata and the data using the same mechanism, namely a
        // lookup of the fields in the database. Alternatively, we could have separate storage and lookup
        // of data and metadata.

        json.put(NAME, getName());

        json.put(PRIMARY_NAMESERVERS, new JSONArray(getPrimaryNameservers()));
        json.put(ACTIVE_NAMESERVERS, new JSONArray(activeNameservers));

        // new fields
        json.put(OLD_ACTIVE_NAMESERVERS, new JSONArray(oldActiveNameservers));
        json.put(OLD_ACTIVE_NAMESERVERS_RUNNING, oldActiveRunning);
        json.put(ACTIVE_NAMESERVERS_RUNNING, activeRunning);
        json.put(ACTIVE_PAXOS_ID, activePaxosID);
        json.put(OLD_ACTIVE_PAXOS_ID, oldActivePaxosID);
        json.put(MARKED_FOR_REMOVAL, markedForRemoval);
        //		json.put(key, value)

        json.put(NAMESERVER_VOTES_MAP, nameServerVotesMap);
        json.put(NAMESERVERSTATSMAP, statsMapToJSONObject(nameServerStatsMap));

        json.put(TOTALAGGREGATEREADFREQUENCY, totalAggregateReadFrequency);
        json.put(TOTALAGGREGATEWRITEFREQUENCY, totalAggregateWriteFrequency);
        json.put(PREVIOUSAGGREAGATEREADFREQUENCY, previousAggregateReadFrequency);
        json.put(PREVIOUSAGGREAGATEWRITEFREQUENCY, previousAggregateWriteFrequency);


        if (movingAvgAggregateLookupFrequency != null) {
            json.put(MOVINGAGGREGATELOOKUPFREQUENCY, movingAvgAggregateLookupFrequency.toJSONArray());
        }
        if (movingAvgAggregateUpdateFrequency != null) {
            json.put(MOVINGAGGREGATEUPDATEFREQUENCY, movingAvgAggregateUpdateFrequency.toJSONArray());
        }

        return json;
    }


    /**
     * @return the name
     */
    public synchronized String getName() {
        return name;
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
     * Returns a copy of the active name servers set.
     */
    public synchronized int numActiveNameServers() { //synchronized
        return activeNameservers.size();
    }


    public synchronized HashSet<Integer> getPrimaryNameservers() {
//		if (StartNameServer.debugMode) GNRS.getLogger().fine("Primaries in name record object: " + primaryNameservers.toString());
        return primaryNameservers;
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

    /**
     * Returns true if the name record contains a primary name server with the given id. False otherwise.
     *
     * @param id Primary name server id
     */
    public synchronized boolean containsPrimaryNameserver(int id) {
        return getPrimaryNameservers().contains(id);
    }

    /**
     *
     * @param id
     * @param readFrequency
     * @param writeFrequency
     */
    public synchronized void addNameServerStats(int id, int readFrequency, int writeFrequency) {
        if (nameServerStatsMap != null) {
            nameServerStatsMap.put(id, new StatsInfo(readFrequency, writeFrequency));
        }
    }

    /**
     * Adds vote to the name server for replica selection.
     *
     * @param id Name server id receiving the vote
     */
    public synchronized void addReplicaSelectionVote(int id, int vote) { //synchronized
        if (nameServerVotesMap.containsKey(id)) {
            int votes = nameServerVotesMap.get(id) + vote;
            nameServerVotesMap.put(id, votes);
        } else {
            nameServerVotesMap.put(id, vote);
        }
    }


    /**
     * Returns a set of highest voted name servers ids.
     *
     * @param numReplica Number of name servers to be selected.
     */
    public synchronized Set<Integer> getHighestVotedReplicaID(int numReplica) { //synchronized
        Set<Integer> replicas = new HashSet<Integer>();

        for (int i = 1; i <= numReplica; i++) {
            int highestVotes = -1;
            int highestVotedReplicaID = -1;

            for (Map.Entry<Integer, Integer> entry : nameServerVotesMap.entrySet()) {
                int nameServerId = entry.getKey();
                int votes = entry.getValue();
                //Skip name server that are unreachable
                // from main branch 269
                if (ConfigFileInfo.getPingLatency(nameServerId) == -1
                        || getPrimaryNameservers().contains(nameServerId)) {
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

            if (replicas.size() == nameServerVotesMap.size()) {
                break;
            }
        }
        return replicas;
    }

    /**
     * Returns the total number of lookup request across all active name servers
     *
     * @return
     */
    public synchronized double getReadStats_Paxos() {
        totalAggregateReadFrequency = 0;
        for (StatsInfo info : nameServerStatsMap.values()) {
            totalAggregateReadFrequency += info.read;
        }

        if (StartNameServer.debugMode) {
            GNS.getLogger().fine("TotalAggregateRead: " + totalAggregateReadFrequency
                    + " Previous: " + previousAggregateReadFrequency
                    + " Current: " + (totalAggregateReadFrequency - previousAggregateReadFrequency));
        }
        movingAvgAggregateLookupFrequency.add(totalAggregateReadFrequency - previousAggregateReadFrequency);
        previousAggregateReadFrequency = totalAggregateReadFrequency;


        //      if (currentReadFrequency > 0) {
        //
        //      }
        return movingAvgAggregateLookupFrequency.getAverage();
        //      return Util.round();
    }

    /**
     * Returns the total number of updates request across all active name servers
     *
     * @return
     */
    public synchronized double getWriteStats_Paxos() {
        totalAggregateWriteFrequency = 0;
        for (StatsInfo info : nameServerStatsMap.values()) {
            totalAggregateWriteFrequency += info.write;
        }

        if (StartNameServer.debugMode) {
            GNS.getLogger().fine("TotalAggregateWrite: " + totalAggregateWriteFrequency
                    + " Previous: " + previousAggregateWriteFrequency
                    + " Current: " + (totalAggregateWriteFrequency - previousAggregateWriteFrequency));
        }
        movingAvgAggregateUpdateFrequency.add(totalAggregateWriteFrequency - previousAggregateWriteFrequency);
        previousAggregateWriteFrequency = totalAggregateWriteFrequency;

        return movingAvgAggregateUpdateFrequency.getAverage();
        //      return Util.round();
    }


    private static ConcurrentHashMap<Integer, Integer> toIntegerMap(JSONObject json) {
        HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
        try {
            Iterator<String> nameItr = json.keys();
            while (nameItr.hasNext()) {
                String name = nameItr.next();
                map.put(Integer.valueOf(name), json.getInt(name));
            }
        } catch (JSONException e) {
        }
        return new ConcurrentHashMap<Integer, Integer>(map);
    }

    private static ConcurrentHashMap<Integer, StatsInfo> toStatsMap(JSONObject json) { //synchronized
        HashMap<Integer, StatsInfo> map = new HashMap<Integer, StatsInfo>();
        try {
            Iterator<String> nameItr = json.keys();
            while (nameItr.hasNext()) {
                String name = nameItr.next();
                map.put(Integer.valueOf(name), new StatsInfo(json.getJSONObject(name)));
            }
        } catch (JSONException e) {
        }
        return new ConcurrentHashMap<Integer, StatsInfo>(map);
    }

    public synchronized JSONObject statsMapToJSONObject(ConcurrentMap<Integer, StatsInfo> map) {
        JSONObject json = new JSONObject();
        try {
            if (map != null) {
                for (Map.Entry<Integer, StatsInfo> e : map.entrySet()) {
                    StatsInfo value = e.getValue();
                    if (value != null) {
                        JSONObject jsonStats = new JSONObject();
                        jsonStats.put("read", value.read);
                        jsonStats.put("write", value.write);
                        json.put(e.getKey().toString(), jsonStats);
                    }
                }
            }
        } catch (JSONException e) {
        }
        return json;
    }

    /**
     * return the set of active name servers in the network.
     *
     * @return
     */
    public synchronized Set<Integer> getOldActiveNameServers() {
        return oldActiveNameservers;
    }

    /**
     * @return the isPrimaryReplica
     */
    public synchronized boolean isPrimaryReplica() {
        return primaryNameservers.contains(NameServer.nodeID);
    }


    /**
     * Makes current active name servers old active name servers, and sets the new active name servers to true.
     *
     * @param newActiveNameServers
     * @param newActivePaxosID
     */
    public synchronized void updateActiveNameServers(Set<Integer> newActiveNameServers, String newActivePaxosID) {
        this.oldActiveRunning = this.activeRunning;
        this.oldActiveNameservers = this.activeNameservers;
        this.oldActivePaxosID = this.activePaxosID;
        this.activeNameservers = newActiveNameServers;
        this.activeRunning = false;
        this.activePaxosID = newActivePaxosID;
    }



    /**
     * are oldActiveNameSevers stopped now?
     *
     * @return
     */
    public synchronized boolean isOldActiveStopped(String oldPaxosID) {
        // checks whether old paxos is still running
        if (this.oldActivePaxosID.equals(oldPaxosID)) {
            // return database status
            return !oldActiveRunning;
        }
        // if oldActivePaxosID != oldPaxosID, it means this oldPaxosID had stopped.
        return true;
    }

    /**
     * are the activeNameServers running?
     *
     * @return
     */
    public synchronized boolean isActiveRunning() {
        return activeRunning;
    }

    public synchronized boolean setOldActiveStopped(String oldActiveID) {
        if (oldActiveID.equals(this.oldActivePaxosID)) {
            this.oldActiveRunning = false;
            return true;
        }
        return false;
    }

    /**
     * Set new active running.
     *
     * @param newActiveID
     * @return
     */
    public synchronized boolean setNewActiveRunning(String newActiveID) {
        if (newActiveID.equals(this.activePaxosID)) {
            this.activeRunning = true;
            return true;
        }
        return false;
    }

    /**
     * return paxos ID for the oldActiveNameSevers
     *
     * @return
     */
    public synchronized String getOldActivePaxosID() {
        return oldActivePaxosID;
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
     * primary checks the current step of transition from old active name servers to new active name servers
     *
     * @return
     */
    public synchronized ACTIVE_STATE getNewActiveTransitionStage() {
        if (!activeRunning && oldActiveRunning) {
            return ACTIVE_STATE.OLD_ACTIVE_RUNNING; // new proposed but old not stop stopped
        }
        if (!activeRunning && !oldActiveRunning) {
            return ACTIVE_STATE.NO_ACTIVE_RUNNING; // both are stopped, start new active
        }
        if (activeRunning && !oldActiveRunning) {
            return ACTIVE_STATE.ACTIVE_RUNNING; // normal operation
        }
        if (StartNameServer.debugMode) {
            GNS.getLogger().severe("ERROR: BOTH OLD & NEW ACTIVE RUNNING for NameRecord. An error condition.");
        }
        return ACTIVE_STATE.BOTH_ACTIVE_RUNNING_ERROR; // both cannot be running.
    }

    /**
     * whether the flag is set of not.
     *
     * @return
     */
    public synchronized boolean isMarkedForRemoval() {
        return markedForRemoval;
    }

    /**
     * sets the flag at name record stored at primary that this name record will be removed soon.
     */
    public synchronized void setMarkedForRemoval() {
        this.markedForRemoval = true;
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
