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
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import java.net.InetSocketAddress;
import org.json.JSONArray;
import static org.junit.Assert.*;
import org.junit.Test;

public class WriteSizeTest {

  private static final String ACCOUNT_ALIAS = "westy@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static UniversalTcpClientExtended client;
  private static GuidEntry masterGuid;

  public WriteSizeTest() {
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
  public void writeSizeTest() {
    int numValues = 100;
    int valueSizeIncrement = 50000;
    GuidEntry tempEntry = null;
    System.out.println("Writing values to field with sizes from " + valueSizeIncrement + " to " + valueSizeIncrement * 100 +
            " by increments of " + valueSizeIncrement);
    try {
      String guidName = "testGUID" + RandomString.randomString(20);
      System.out.println("Creating guid: " + guidName);
      tempEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, guidName);
    } catch (Exception e) {
      fail("Exception creating guid: " + e);
    }

    String fieldName = "testField" + RandomString.randomString(10);
    String fieldValue = RandomString.randomString(10);
    System.out.println("Creating field: " + fieldName);
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
    for (int k = 0; k < numValues - 1; k++) {
      String value = RandomString.randomString(valueSizeIncrement * k + 1);
      System.out.println("Writing value of length " + value.length());
      try {
        client.fieldReplace(tempEntry, fieldName, value);
      } catch (Exception e) {
        fail("Exception appending value onto field: " + e);
      }
      JSONArray array = null;
      try {
        array = client.fieldReadArray(tempEntry.getGuid(), fieldName, tempEntry);
      } catch (Exception e) {
        fail("Exception appending value onto field: " + e);
      }
    }
    
  }
}
