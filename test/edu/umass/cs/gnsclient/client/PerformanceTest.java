/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnscommon.utils.RandomString;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import org.json.JSONArray;
import static org.junit.Assert.*;
import org.junit.Test;

public class PerformanceTest {

  private static final String ACCOUNT_ALIAS = "westy@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static UniversalTcpClientExtended client;
  private static GuidEntry masterGuid;

  public PerformanceTest() {
    if (client == null) {
      InetSocketAddress address = ServerSelectDialog.selectServer();
      client = new UniversalTcpClientExtended(address.getHostName(), address.getPort(), true);
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        fail("Exception when we were not expecting it: " + e);
      }
    }
  }

  @Test
  public void performanceTest() {
    int numGuids = 1;
    int numFields = 10;
    int numValues = 100;
    int valueSizeIncrement = 10000;
    System.out.println("Creating " + numGuids + " guids each with " + numFields + " fields. Each field has "
            + numValues + " of size " + valueSizeIncrement + " chars incrementally added to it.");
    for (int i = 0; i < numGuids; i++) {
      GuidEntry tempEntry = null;
      try {
        String guidName = "testGUID" + RandomString.randomString(20);
        System.out.println("Creating guid: " + guidName);
        tempEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, guidName);
      } catch (Exception e) {
        fail("Exception creating guid: " + e);
      }

      for (int j = 0; j < numFields; j++) {
        String fieldName = RandomString.randomString(10);
        String fieldValue = RandomString.randomString(10);
        System.out.println("Creating field #" + j + " : " + fieldName);
        try {
          client.fieldCreateOneElementList(tempEntry, fieldName, fieldValue);
        } catch (Exception e) {
          fail("Exception creating field: " + e);
        }
        try {
          client.fieldReadArray(tempEntry.getGuid(), fieldName, tempEntry);
        } catch (Exception e) {
          fail("Exception reading field: " + e);
        }
        ArrayList<String> allValues = new ArrayList<String>();
        for (int k = 0; k < numValues; k++) {
          allValues.add(RandomString.randomString(valueSizeIncrement));
        }
        try {
          client.fieldAppend(tempEntry.getGuid(), fieldName, new JSONArray(allValues), tempEntry);
        } catch (Exception e) {
          fail("Exception appending value onto field: " + e);
        }
        JSONArray array = null;
        try {
          array = client.fieldReadArray(tempEntry.getGuid(), fieldName, tempEntry);
        } catch (Exception e) {
          fail("Exception appending value onto field: " + e);
        }
        System.out.println("Length of array = " + array.toString().length());
      }
      System.out.println("Done with guid #" + i);
    }
  }
}
