/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientprotocol;

import edu.umass.cs.gns.client.AccountAccess;
import edu.umass.cs.gns.client.FieldAccess;
import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.util.Stats;
import edu.umass.cs.gns.util.Util;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;

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
  public static String runRttPerformanceTest(int numFields) {
    // Put things in this string to report our results.
    StringBuilder result = new StringBuilder();
    ArrayList<String> fields = new ArrayList<String>();
    ArrayList<Double> times = new ArrayList<Double>();

    try {
      String guid;
      // see if we already registered our GUID
      if ((guid = AccountAccess.lookupGuid(ACCOUNTNAME)) == null) {
        // if not we use the method  below which bypasses the normal email verification requirement
        // but first we create a GUID from our public key
        guid = ClientUtils.createGuidFromPublicKey(PUBLICKEY);
        AccountAccess.addAccount(ACCOUNTNAME, guid, PUBLICKEY, "", false);
      }

      // Create n random fields with random values first. Do all of them before we do the reads
      // so the GNS has time to "settle".
      for (int i = 0; i < numFields; i++) {
        String field = "RTT-" + Util.randomString(7);
        if (FieldAccess.create(guid, field, new ResultValue(Arrays.asList(Util.randomString(7))))) {
          fields.add(field);
        } else {
          result.append("Unable to create " + field);
          result.append(NEWLINE);
        }
      }
      // Next go through all the fields we just created and read the values back,
      // acessing the RoundTripTime fields of the ValuesMap class which records the
      // time between the LNS sending the request to the NS and the return message.
      for (String field : fields) {
        ValuesMap value = Intercessor.sendMultipleReturnValueQuery(guid, field, true, null, null, null);
        if (value != null) {
          //result.append(value.getRoundTripTime());
          times.add(new Double(value.getRoundTripTime()));
        } else {
          //result.append(field + " is " + null);
        }
        //result.append(NEWLINE);
      }
      // Put some statistics in the results string.
      result.append("N = " + times.size());
      result.append(NEWLINE);
      Stats stats = new Stats(times);
      result.append("Avg = " + stats.getMean());
      result.append(NEWLINE);
      result.append("StdDev = " + stats.getStdDev());
      result.append(NEWLINE);
    } catch (Exception e) {
      result.append("Error during RTT Test: ");
      result.append(e.getMessage());
      result.append(NEWLINE);
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      result.append(sw.toString());
      result.append(NEWLINE);
    }
    return result.toString();
  }
}
