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

import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import static edu.umass.cs.gnscommon.GnsProtocol.ACCOUNT_GUID;
import static edu.umass.cs.gnscommon.GnsProtocol.ADD_MULTIPLE_GUIDS;
import static edu.umass.cs.gnscommon.GnsProtocol.GUIDCNT;
import static edu.umass.cs.gnscommon.GnsProtocol.NAMES;
import static edu.umass.cs.gnscommon.GnsProtocol.PUBLIC_KEYS;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.utils.DelayProfiler;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Basic test for the GNS using the UniversalTcpClient.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreateGuidBatchTcpClientWithSSLTest {

  private static UniversalTcpClientExtended client;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;

  public CreateGuidBatchTcpClientWithSSLTest() {

    if (address == null) {
      if (System.getProperty("host") != null
              && !System.getProperty("host").isEmpty()
              && System.getProperty("port") != null
              && !System.getProperty("port").isEmpty()) {
        address = new InetSocketAddress(System.getProperty("host"),
                Integer.parseInt(System.getProperty("port")));
      } else {
        address = ServerSelectDialog.selectServer();
      }
      client = new UniversalTcpClientExtended(address.getHostName(), address.getPort());
    }
  }

  @Test
  public void test_01_CreateBatch() {
    GuidEntry masterGuid = null;
    try {
      String batchAccountAlias = "batchTest" + RandomString.randomString(6) + "@gns.name";
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, batchAccountAlias, "password", true);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
    int numberTocreate = 100;
    if (System.getProperty("count") != null
            && !System.getProperty("count").isEmpty()) {
      numberTocreate = Integer.parseInt(System.getProperty("count"));
    }
    Set<String> aliases = new HashSet<>();
    for (int i = 0; i < numberTocreate; i++) {
      aliases.add("testGUID" + RandomString.randomString(6));
    }
    String result = null;
    int oldTimeout = client.getReadTimeout();
    try {
      client.setReadTimeout(15 * 1000); // 30 seconds
      result = client.guidBatchCreate(masterGuid, aliases, true);
      client.setReadTimeout(oldTimeout);
    } catch (Exception e) {
      fail("Exception while creating guids: " + e);
    }
    assertEquals(GnsProtocol.OK_RESPONSE, result);
    try {
      JSONObject accountRecord = client.lookupAccountRecord(masterGuid.getGuid());
      assertEquals(numberTocreate, accountRecord.getInt("guidCnt"));
    } catch (JSONException | GnsException | IOException e) {
      fail("Exception while fetching account record: " + e);
    }
  }

  @Test
  public void test_02_CreateBatchFast() {
    GuidEntry masterGuid = null;
    try {
      String batchAccountAlias = "batchTest" + RandomString.randomString(6) + "@gns.name";
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, batchAccountAlias, "password", true);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
    int numberTocreate = 100;
    if (System.getProperty("count") != null
            && !System.getProperty("count").isEmpty()) {
      numberTocreate = Integer.parseInt(System.getProperty("count"));
    }
    Set<String> aliases = new HashSet<>();
    for (int i = 0; i < numberTocreate; i++) {
      aliases.add("testGUID" + RandomString.randomString(6));
    }
    String result = null;
    int oldTimeout = client.getReadTimeout();
    try {
      client.setReadTimeout(60 * 1000); // 30 seconds
      result = client.guidBatchCreate(masterGuid, aliases, false);
      client.setReadTimeout(oldTimeout);
    } catch (Exception e) {
      fail("Exception while creating guids: " + e);
    }
    assertEquals(GnsProtocol.OK_RESPONSE, result);
    try {
      JSONObject accountRecord = client.lookupAccountRecord(masterGuid.getGuid());
      assertEquals(numberTocreate, accountRecord.getInt("guidCnt"));
    } catch (JSONException | GnsException | IOException e) {
      fail("Exception while fetching account record: " + e);
    }
  }

  @Test
  public void test_03_CreateBatchFastest() {
    GuidEntry masterGuid = null;
    try {
      String batchAccountAlias = "batchTest" + RandomString.randomString(6) + "@gns.name";
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, batchAccountAlias, "password", true);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
    int numberTocreate = 100;
    if (System.getProperty("count") != null
            && !System.getProperty("count").isEmpty()) {
      numberTocreate = Integer.parseInt(System.getProperty("count"));
    }
    String result = null;
    int oldTimeout = client.getReadTimeout();
    try {
      client.setReadTimeout(60 * 1000); // 30 seconds
      result = client.guidBatchCreateFast(masterGuid, numberTocreate);
      client.setReadTimeout(oldTimeout);
    } catch (Exception e) {
      fail("Exception while creating guids: " + e);
    }
    assertEquals(GnsProtocol.OK_RESPONSE, result);
    try {
      JSONObject accountRecord = client.lookupAccountRecord(masterGuid.getGuid());
      assertEquals(numberTocreate, accountRecord.getInt("guidCnt"));
    } catch (JSONException | GnsException | IOException e) {
      fail("Exception while fetching account record: " + e);
    }
  }

}
