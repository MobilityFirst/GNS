package edu.umass.cs.contextservice.integration.examples;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;


public class ContextServiceExample {
  // dallas region in texas area, for which we have weather alerts.


  public static final double LONGITUDE_MIN = -98.08;


  public static final double LONGITUDE_MAX = -96.01;


  public static final double LATITUDE_MAX = 33.635;


  public static final double LATITUDE_MIN = 31.854;


  public static final double LEFT = LONGITUDE_MIN;


  public static final double RIGHT = LONGITUDE_MAX;


  public static final double TOP = LATITUDE_MAX;


  public static final double BOTTOM = LATITUDE_MIN;


  public static final long QUERY_EXPIRY_TIME = 5 * 60 * 1000; // 5 min in ms


  public static final String ACCOUNT_ALIAS = "admincontextservice@gns.name";


  public static GNSClientCommands client;


  public static ContextServiceClient<Integer> csClient;


  public static GuidEntry account_guid;


  public static void main(String[] args) throws Exception {
    String csHost = args[0];
    int csPort = Integer.parseInt(args[1]);

    client = new GNSClientCommands();
    System.out.println("[Client connected to GNS]\n");

    try {

      System.out
              .println("// account GUID creation\n"
                      + "GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS,"
                      + " \"password\", true)\n");
      account_guid = GuidUtils.lookupOrCreateAccountGuid(client,
              ACCOUNT_ALIAS, "password", true);
    } catch (Exception | Error e) {
      System.out.println("Exception during accountGuid creation: " + e);
      e.printStackTrace();
      System.exit(1);
    }

    System.out.println("csHost " + csHost + " csPort " + csPort);

    csClient = new ContextServiceClient<Integer>(csHost, csPort);

    String csSearchQuery = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "
            + "latitude >= 32 AND latitude <= 33 AND "
            + "longitude >= -98 AND longitude <= -97";

    System.out.println("\n Issuing a search query to context service "
            + csSearchQuery + "\n");

    JSONArray resultArray = new JSONArray();

    int replySize = csClient.sendSearchQuery(csSearchQuery, resultArray, QUERY_EXPIRY_TIME);

    System.out.println("Search query reply size " + replySize + " GUIDs " + resultArray + "\n");

    JSONObject userJSON = new JSONObject();
    // user setting location inside the search query area.
    userJSON.put("latitude", 32.5);
    userJSON.put("longitude", -97.5);

    System.out.println("A user with GUID " + account_guid.getGuid()
            + " entering the geofence specified in the search query by doing an update"
            + ", latitude=32.5 and longitude=-97.5, in GNS\n");

    client.update(account_guid, userJSON);

    // checking for trigger from context service that a new guid is added in an
    // already existing group. This call blocks until there are non zero triggers.
    JSONArray triggerArray = new JSONArray();
    csClient.getQueryUpdateTriggers(triggerArray);

    System.out.println("Context service sends a trigger showing that user with GUID "
            + account_guid.getGuid() + " enters the geofence, the geofence's group GUID is in the TO_BE_ADDED field\n");
    for (int i = 0; i < triggerArray.length(); i++) {
      System.out.println("Trigger JSON " + triggerArray.getJSONObject(i));
    }

    userJSON = new JSONObject();
    // user setting location inside the search query area.
    userJSON.put("latitude", 31);
    userJSON.put("longitude", -97.5);

    System.out.println("\n A user with guid" + account_guid.getGuid()
            + " going out of the geofence specified in the search query by doing an update, latitude=31 and longitude=-97.5,  in GNS\n");
    client.update(account_guid, userJSON);

    // checking for trigger from context service that the guid is removed from an
    // already existing group. This call blocks until there are non zero triggers.
    triggerArray = new JSONArray();
    csClient.getQueryUpdateTriggers(triggerArray);

    System.out.println("Context service sends a trigger showing that user with GUID "
            + account_guid.getGuid() + " removed from the geofence, the geofence's group GUID is in TO_BE_REMOVED field ");
    for (int i = 0; i < triggerArray.length(); i++) {
      System.out.println("Trigger JSON " + triggerArray.getJSONObject(i));
    }

    System.out.println("\nBasic GNS CS test successful\n");
    System.exit(0);
  }
}
