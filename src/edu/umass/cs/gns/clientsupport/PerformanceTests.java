/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.Stats;
import edu.umass.cs.gns.util.Util;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 
 * This class contains the code for various server side client support tests.
 * The tests in here are invoked out of the Protocol class in response to
 * requests that come from the HTTP service.
 * 
 *
 * @author westy
 */
public class PerformanceTests {

  private static final String ACCOUNTNAME = "RoundTripPerformanceTest";
  private static final String PUBLICKEY = "RTT";
  private static final String NEWLINE = System.getProperty("line.separator");
  private static Map<String, ArrayList<Double>> times;

  /**
   * This method implements the Round Trip time test. 
   * 
   * We rely on the ValuesMap class's getRoundTripTime method which
   * records the time between the LNS sending the request to the NS 
   * and reception of the return message containing the answer.
   * 
   * We implement this here as opposed to in a client test because
   * here we can avoid having to do GUID verification and message signing.
   * 
   * @param numFields
   * @param guidCnt
   * @param verbose
   * @return 
   */
  public static String runRttPerformanceTest(int numFields, int guidCnt, boolean verbose, ClientRequestHandlerInterface handler) {
    StringBuilder result = new StringBuilder();
    String accountGuid;
    // see if we already registered our GUID
    if ((accountGuid = AccountAccess.lookupGuid(ACCOUNTNAME, handler)) == null) {
      // if not we use the method  below which bypasses the normal email verification requirement
      // but first we create a GUID from our public key
      accountGuid = ClientUtils.createGuidFromPublicKey(PUBLICKEY.getBytes());
      AccountAccess.addAccount(ACCOUNTNAME, accountGuid, PUBLICKEY, "", false, handler);
    }
    times = new HashMap<String, ArrayList<Double>>();
    if (verbose) {
      result.append("GUIDs:");
      result.append(NEWLINE);
    }
    for (String guid : createSomeGuids(AccountAccess.lookupAccountInfoFromGuid(accountGuid, handler), guidCnt, handler)) {
      if (verbose) {
        result.append(guid);
        result.append(NEWLINE);
      }
      runTestForGuid(guid, numFields, handler);
    }
    //result.append(runTestForGuid(accountGuid, numFields));
    if (verbose) {
      result.append(resultsToString(handler));
    } else {
      String checkResult = checkForExcessiveRtt(handler);
      if (checkResult.isEmpty()) {
        result.append("All RTTs within specified parameters.");
      } else {
        result.append(checkResult);
      }
    }
    return result.toString();
  }

  private static ArrayList<String> createSomeGuids(AccountInfo info, int count, ClientRequestHandlerInterface handler) {
    ArrayList<String> result = new ArrayList<String>();

    for (int i = 0; i < count; i++) {
      String name = "RTT-" + Util.randomString(6);
      String publicKey = name + "-KEY";
      String guid = ClientUtils.createGuidFromPublicKey(publicKey.getBytes());
      AccountAccess.addGuid(info, name, guid, publicKey, handler);
      result.add(guid);
    }
    return result;
  }

  private static void runTestForGuid(String guid, int numFields, ClientRequestHandlerInterface handler) {
    ArrayList<String> fields = new ArrayList<String>();
    try {
      // Create n random fields with random values first. Do all of them before we do the reads
      // so the GNS has time to "settle".
      for (int i = 0; i < numFields; i++) {
        String field = "RTT-" + Util.randomString(7);
        if (!FieldAccess.create(guid, field, new ResultValue(Arrays.asList(Util.randomString(7))),
                // ignore signature for now
                null, null, null, handler).isAnError()) {
          fields.add(field);
        }
      }
      // Next go through all the fields we just created and read the values back,
      // acessing the RoundTripTime fields of the ValuesMap class which records the
      // time between the LNS sending the request to the NS and the return message.
      for (String field : fields) {
        QueryResult value = handler.getIntercessor().sendQuery(guid, field, null, null, null, ColumnFieldType.LIST_STRING);
        if (!value.isError()) {
          //result.append(value.getRoundTripTime());
          if (times.get(value.getResponder()) == null) {
            times.put((String)value.getResponder(), new ArrayList<Double>());
          }
          times.get(value.getResponder()).add(new Double(value.getRoundTripTime()));
        } else {
        }
      }
    } catch (Exception e) {
    }
  }

  private static int getComparisonPingValue(String node, ClientRequestHandlerInterface handler) {
    String result = handler.getAdmintercessor().sendPingValue(PingManager.LOCALNAMESERVERID, node, handler);
    if (result.startsWith(Defs.BADRESPONSE)) {
      return 999;
    } else {
      return Integer.parseInt(result);
    }
  }
  
  private static final int EXCESSIVE_RTT_DIFFERENCE = 100; 
  
  private static String checkForExcessiveRtt(ClientRequestHandlerInterface handler) {
    StringBuilder result = new StringBuilder();
    for (Entry<String, ArrayList<Double>> entry : times.entrySet()) {
      String node = entry.getKey();
      Stats stats = new Stats(times.get(node));
      int avg = (int) Math.round(stats.getMean());
      int ping = getComparisonPingValue(node, handler);
      if (avg - ping > EXCESSIVE_RTT_DIFFERENCE) {
        result.append("Node ");
        result.append(entry.getKey().toString());
        result.append(" has excessive rtt of ");
        result.append(avg);
        result.append("ms (ping is ");
        result.append(ping);
        result.append("ms) ");
        result.append(NEWLINE);
      }
    }
    return result.toString();
  }

  private static String resultsToString(ClientRequestHandlerInterface handler) {
    StringBuilder result = new StringBuilder();
    for (Entry<String, ArrayList<Double>> entry : times.entrySet()) {
      Stats stats = new Stats(times.get(entry.getKey()));
      result.append(NEWLINE);
      result.append("Node: ");
      result.append(entry.getKey().toString());
      result.append(NEWLINE);
      result.append("Fields read = " + stats.getN());
      result.append(NEWLINE);
      result.append("Avg RTT = " + Math.round(stats.getMean()) + "ms");
      result.append(NEWLINE);
      result.append("StdDev = " + Math.round(stats.getStdDev()) + "ms");
      result.append(NEWLINE);
      result.append("Ping = " + getComparisonPingValue(entry.getKey(), handler) + "ms");
      result.append(NEWLINE);
    }
    return result.toString();
  }
}
