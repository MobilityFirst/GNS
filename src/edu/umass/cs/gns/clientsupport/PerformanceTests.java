/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.localnameserver.LocalNameServer;
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
  private static Map<Integer, ArrayList<Double>> times;

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
   * @return 
   */
  public static String runRttPerformanceTest(int numFields, int guidCnt, boolean verbose) {
    StringBuilder result = new StringBuilder();
    String accountGuid;
    // see if we already registered our GUID
    if ((accountGuid = AccountAccess.lookupGuid(ACCOUNTNAME)) == null) {
      // if not we use the method  below which bypasses the normal email verification requirement
      // but first we create a GUID from our public key
      accountGuid = ClientUtils.createGuidFromPublicKey(PUBLICKEY);
      AccountAccess.addAccount(ACCOUNTNAME, accountGuid, PUBLICKEY, "", false);
    }
    times = new HashMap<Integer, ArrayList<Double>>();
    if (verbose) {
      result.append("GUIDs:");
      result.append(NEWLINE);
    }
    for (String guid : createSomeGuids(AccountAccess.lookupAccountInfoFromGuid(accountGuid), guidCnt)) {
      if (verbose) {
        result.append(guid);
        result.append(NEWLINE);
      }
      runTestForGuid(guid, numFields);
    }
    //result.append(runTestForGuid(accountGuid, numFields));
    if (verbose) {
      result.append(resultsToString());
    } else {
      String checkResult = checkForExcessiveRtt();
      if (checkResult.isEmpty()) {
        result.append("All RTTs within specified parameters.");
      } else {
        result.append(checkResult);
      }
    }
    return result.toString();
  }

  private static ArrayList<String> createSomeGuids(AccountInfo info, int count) {
    ArrayList<String> result = new ArrayList<String>();

    for (int i = 0; i < count; i++) {
      String name = "RTT-" + Util.randomString(6);
      String publicKey = name + "-KEY";
      String guid = ClientUtils.createGuidFromPublicKey(publicKey);
      AccountAccess.addGuid(info, name, guid, publicKey);
      result.add(guid);
    }
    return result;
  }

  private static void runTestForGuid(String guid, int numFields) {
    ArrayList<String> fields = new ArrayList<String>();
    try {
      // Create n random fields with random values first. Do all of them before we do the reads
      // so the GNS has time to "settle".
      for (int i = 0; i < numFields; i++) {
        String field = "RTT-" + Util.randomString(7);
        if (!FieldAccess.create(guid, field, new ResultValue(Arrays.asList(Util.randomString(7))),
                // ignore signature for now
                null, null, null).isAnError()) {
          fields.add(field);
        }
      }
      // Next go through all the fields we just created and read the values back,
      // acessing the RoundTripTime fields of the ValuesMap class which records the
      // time between the LNS sending the request to the NS and the return message.
      for (String field : fields) {
        QueryResult value = Intercessor.sendQuery(guid, field, null, null, null);
        if (!value.isError()) {
          //result.append(value.getRoundTripTime());
          if (times.get(value.getResponder()) == null) {
            times.put(value.getResponder(), new ArrayList<Double>());
          }
          times.get(value.getResponder()).add(new Double(value.getRoundTripTime()));
        } else {
        }
      }
    } catch (Exception e) {
    }
  }

  private static int getComparisonPingValue(int node) {
    String result = Admintercessor.sendPingValue(LocalNameServer.getNodeID(), node);
    if (result.startsWith(Defs.BADRESPONSE)) {
      return 999;
    } else {
      return Integer.parseInt(result);
    }
  }
  
  private static final int EXCESSIVE_RTT_DIFFERENCE = 100; 
  
  private static String checkForExcessiveRtt() {
    StringBuilder result = new StringBuilder();
    for (Entry<Integer, ArrayList<Double>> entry : times.entrySet()) {
      int node = entry.getKey();
      Stats stats = new Stats(times.get(node));
      int avg = (int) Math.round(stats.getMean());
      int ping = getComparisonPingValue(node);
      if (avg - ping > EXCESSIVE_RTT_DIFFERENCE) {
        result.append("Node ");
        result.append(entry.getKey());
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

  private static String resultsToString() {
    StringBuilder result = new StringBuilder();
    for (Entry<Integer, ArrayList<Double>> entry : times.entrySet()) {
      Stats stats = new Stats(times.get(entry.getKey()));
      result.append(NEWLINE);
      result.append("Node: ");
      result.append(entry.getKey());
      result.append(NEWLINE);
      result.append("Fields read = " + stats.getN());
      result.append(NEWLINE);
      result.append("Avg RTT = " + Math.round(stats.getMean()) + "ms");
      result.append(NEWLINE);
      result.append("StdDev = " + Math.round(stats.getStdDev()) + "ms");
      result.append(NEWLINE);
      result.append("Ping = " + getComparisonPingValue(entry.getKey()) + "ms");
      result.append(NEWLINE);
    }
    return result.toString();
  }
}
