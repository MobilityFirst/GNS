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
 * @author westy
 */
public class PerformanceTests {

  private static final String ACCOUNTNAME = "RoundTripPerformanceTest";
  private static final String PUBLICKEY = "RTT";
  private static final String NEWLINE = System.getProperty("line.separator");
  
  private static Intercessor client = Intercessor.getInstance();
  private static FieldAccess fieldAccess = FieldAccess.getInstance();
  private static AccountAccess accountAccess = AccountAccess.getInstance();

  public static String runRttPerformanceTest(int numFields) {
    StringBuilder result = new StringBuilder();
    ArrayList<String> fields = new ArrayList<String>();
    ArrayList<Double> times = new ArrayList<Double>();

    try {
      String guid;
      if ((guid = accountAccess.lookupGuid(ACCOUNTNAME)) == null) {
        guid = ClientUtils.createGuidFromPublicKey(PUBLICKEY);
        accountAccess.addAccount(ACCOUNTNAME, guid, PUBLICKEY, "", false);
      }

      for (int i = 0; i < numFields; i++) {
        String field = "RTT-" + Util.randomString(7);
        if (fieldAccess.create(guid, field, new ResultValue(Arrays.asList(Util.randomString(7))))) {
          fields.add(field);
        } else {
          result.append("Unable to create " + field);
          result.append(NEWLINE);
        }
      }
      for (String field : fields) {
        ValuesMap value = client.sendMultipleReturnValueQuery(guid, field, true);
        if (value != null) {
          //result.append(value.getRoundTripTime());
          times.add(new Double(value.getRoundTripTime()));
        } else {
          //result.append(field + " is " + null);
        }
        //result.append(NEWLINE);
      }
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
