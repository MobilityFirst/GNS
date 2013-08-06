package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 7/26/13
 * Time: 7:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class DBReplicaControllerV1 {

    private static String DBREPLICACONTROLLER = MongoRecords.DBREPLICACONTROLLER;


    public static ReplicaControllerRecord getNameRecordPrimary(String name) {
        try {
            JSONObject json = MongoRecords.getInstance().lookup(DBREPLICACONTROLLER,name);
            if (json == null) return null;
            else return new ReplicaControllerRecord(json);
        } catch (JSONException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return null;
    }

    public static void addNameRecordPrimary(ReplicaControllerRecord recordEntry) {
        if (StartNameServer.debugMode) {
            GNS.getLogger().fine("Start addNameRecord " + recordEntry.getName());
        }

        try {
            MongoRecords.getInstance().insert(DBREPLICACONTROLLER,recordEntry.getName(),recordEntry.toJSONObject());
        } catch (JSONException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return;
        }
    }

    public static void updateNameRecordPrimary(ReplicaControllerRecord recordEntry) {
        try {
            MongoRecords.getInstance().update(DBREPLICACONTROLLER,recordEntry.getName(),recordEntry.toJSONObject());
        } catch (JSONException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    public static void removeNameRecord(String name) {
        MongoRecords.getInstance().remove(DBREPLICACONTROLLER,name);
    }


    public static Set<ReplicaControllerRecord> getAllPrimaryNameRecords() {
        MongoRecords.getInstance().keySet(DBREPLICACONTROLLER);
        MongoRecords records = MongoRecords.getInstance();
        Set<ReplicaControllerRecord> result = new HashSet<ReplicaControllerRecord>();
        for (JSONObject json : records.retrieveAllEntries(DBREPLICACONTROLLER)) {
            try {
                result.add(new ReplicaControllerRecord(json));
            } catch (JSONException e) {
                GNS.getLogger().severe(records.toString() + ":: Error getting name record: " + e);
                e.printStackTrace();
            }
        }
        return result;
//        return MongoRecordMap.g;
    }

    public static void reset() {

    }

}
