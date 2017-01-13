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
package edu.umass.cs.gnsclient.client.singletests;


import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
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
 * Basic test for the GNS using the UniversalTcpClient.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreateGuidBatchTestWithPublicKeys {

  private static GNSClientCommands client;

  /**
   *
   */
  public CreateGuidBatchTestWithPublicKeys() {

    if (client == null) {
       try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
    }
  }

  private static GuidEntry masterGuid = null;
  private static int numberTocreate = 100;

  /**
   *
   */
  @Test
  public void test_01_CreateBatchAccountGuid() {
    try {
      String batchAccountAlias = "batchTest" + RandomString.randomString(6) + "@gns.name";
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, batchAccountAlias, "password", true);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_02_CreateBatch() {

    if (System.getProperty("count") != null
            && !System.getProperty("count").isEmpty()) {
      numberTocreate = Integer.parseInt(System.getProperty("count"));
    }
    Set<String> aliases = new HashSet<>();
    for (int i = 0; i < numberTocreate; i++) {
      aliases.add("testGUID" + RandomString.randomString(6));
    }
    String result = null;
    long oldTimeout = client.getReadTimeout();
    try {
      client.setReadTimeout(15 * 1000); // 30 seconds
      result = client.guidBatchCreate(masterGuid, aliases);
      client.setReadTimeout(oldTimeout);
    } catch (Exception e) {
      fail("Exception while creating guids: " + e);
    }
    assertEquals(GNSProtocol.OK_RESPONSE.toString(), result);
  }

  /**
   *
   */
  @Test
  public void test_03_CheckBatch() {
    try {
      JSONObject accountRecord = client.lookupAccountRecord(masterGuid.getGuid());
      assertEquals(numberTocreate, accountRecord.getInt("guidCnt"));
    } catch (JSONException | ClientException | IOException e) {
      fail("Exception while fetching account record: " + e);
    }
  }
}
