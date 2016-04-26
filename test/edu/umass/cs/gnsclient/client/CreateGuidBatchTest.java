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


import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.GnsClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONException;
import org.json.JSONObject;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Basic test for the GNS using the GnsClient.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreateGuidBatchTest {

  private static GNSClientCommands client;
  private static int numberTocreate = 100;

  public CreateGuidBatchTest() {

    if (client == null) {
     try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
      // can change the number to create on the command line
      if (System.getProperty("count") != null
              && !System.getProperty("count").isEmpty()) {
        numberTocreate = Integer.parseInt(System.getProperty("count"));
      }
    }
  }

  private static GuidEntry masterGuid = null;

  @Test
  public void test_510_CreateBatchAccountGuid() {
    try {
      String batchAccountAlias = "batchTest" + RandomString.randomString(6) + "@gns.name";
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, batchAccountAlias, "password", true);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_511_CreateBatch() {
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
    assertEquals(GNSCommandProtocol.OK_RESPONSE, result);
  }

  @Test
  public void test_512_CheckBatch() {
    try {
      JSONObject accountRecord = client.lookupAccountRecord(masterGuid.getGuid());
      assertEquals(numberTocreate, accountRecord.getInt("guidCnt"));
    } catch (JSONException | GnsClientException | IOException e) {
      fail("Exception while fetching account record: " + e);
    }
  }

  private static GuidEntry masterGuidForWithoutPublicKeys = null;

  @Test
  public void test_520_CreateBatchAccountGuidForWithoutPublicKeys() {
    try {
      String batchAccountAlias = "batchTest" + RandomString.randomString(6) + "@gns.name";
      masterGuidForWithoutPublicKeys = GuidUtils.lookupOrCreateAccountGuid(client, batchAccountAlias, "password", true);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_521_CreateBatchWithoutPublicKeys() {
    Set<String> aliases = new HashSet<>();
    for (int i = 0; i < numberTocreate; i++) {
      aliases.add("testGUID" + RandomString.randomString(6));
    }
    String result = null;
    int oldTimeout = client.getReadTimeout();
    try {
      client.setReadTimeout(15 * 1000); // 30 seconds
      result = client.guidBatchCreate(masterGuidForWithoutPublicKeys, aliases, false);
      client.setReadTimeout(oldTimeout);
    } catch (Exception e) {
      fail("Exception while creating guids: " + e);
    }
    assertEquals(GNSCommandProtocol.OK_RESPONSE, result);
  }

  @Test
  public void test_522_CheckBatchForWithoutPublicKeys() {
    try {
      JSONObject accountRecord = client.lookupAccountRecord(masterGuidForWithoutPublicKeys.getGuid());
      assertEquals(numberTocreate, accountRecord.getInt("guidCnt"));
    } catch (JSONException | GnsClientException | IOException e) {
      fail("Exception while fetching account record: " + e);
    }
  }

  private static GuidEntry masterGuidForFastest = null;

  @Test
  public void test_530_CreateBatchAccountGuidForForFastest() {
    try {
      String batchAccountAlias = "batchTest" + RandomString.randomString(6) + "@gns.name";
      masterGuidForFastest = GuidUtils.lookupOrCreateAccountGuid(client, batchAccountAlias, "password", true);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  @Test
  public void test_531_CreateBatchFastest() {
    String result = null;
    int oldTimeout = client.getReadTimeout();
    try {
      client.setReadTimeout(15 * 1000); // 30 seconds
      result = client.guidBatchCreateFast(masterGuidForFastest, numberTocreate);
      client.setReadTimeout(oldTimeout);
    } catch (Exception e) {
      fail("Exception while creating guids: " + e);
    }
    assertEquals(GNSCommandProtocol.OK_RESPONSE, result);
  }

  @Test
  public void test_532_CheckBatchForFastest() {
    try {
      JSONObject accountRecord = client.lookupAccountRecord(masterGuidForFastest.getGuid());
      assertEquals(numberTocreate, accountRecord.getInt("guidCnt"));
    } catch (JSONException | GnsClientException | IOException e) {
      fail("Exception while fetching account record: " + e);
    }
  }

}
