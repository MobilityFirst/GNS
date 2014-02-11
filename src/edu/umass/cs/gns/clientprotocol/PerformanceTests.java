/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientprotocol;

import edu.umass.cs.gns.client.AccountAccess;
import edu.umass.cs.gns.client.FieldAccess;
import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.clientprotocol.ClientUtils;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.nameserver.ValuesMap;
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
  private static ArrayList<String> fields = new ArrayList<String>();
  private static Intercessor client = Intercessor.getInstance();
  private static FieldAccess fieldAccess = FieldAccess.getInstance();
  private static AccountAccess accountAccess = AccountAccess.getInstance();

  public static String runRttPerformanceTest() {
    StringBuilder result = new StringBuilder();

    try {
      String guid;
      if ((guid = accountAccess.lookupGuid(ACCOUNTNAME)) == null) {
        guid = ClientUtils.createGuidFromPublicKey(PUBLICKEY);
        accountAccess.addAccount(ACCOUNTNAME, guid, PUBLICKEY, "", false);
      }
      int numFields = 10;

      for (int i = 0; i < numFields; i++) {
        String field = "RTT-" + Util.randomString(7);
        if (fieldAccess.create(guid, field, new ResultValue(Arrays.asList(Util.randomString(7))))) {
          fields.add(field);
        } else {
          result.append("Unable to create " + field);
          result.append(edu.umass.cs.gns.clientprotocol.Defs.NEWLINE);
        }
      }
      for (String field : fields) {
        ValuesMap value = client.sendMultipleReturnValueQuery(guid, field, true);
        if (value != null) {
          result.append(value.getRoundTripTime());
        } else {
          result.append(field + " is " + null);
        }
        result.append(edu.umass.cs.gns.clientprotocol.Defs.NEWLINE);
      }
    } catch (Exception e) {
      result.append("Error during RTT Test: ");
      result.append(e.getMessage());
      result.append(edu.umass.cs.gns.clientprotocol.Defs.NEWLINE);
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      result.append(sw.toString());
      result.append(edu.umass.cs.gns.clientprotocol.Defs.NEWLINE);
    }


    return result.toString();
  }
}
