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
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Basic test for the GNS using the UniversalTcpClient.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreateGuidBatchTestWithPublicKeys extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;

  /**
   *
   */
  public CreateGuidBatchTestWithPublicKeys() {

    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
    }
  }

  private static GuidEntry batchAccountGuid = null;
  private static int numberTocreate = 100;

  /**
   *
   */
  @Test
  public void test_01_CreateBatchAccountGuid() {
    try {
      String batchAccountAlias = "batchTest-" + RandomString.randomString(24) + "@gns.name";
      batchAccountGuid = GuidUtils.lookupOrCreateAccountGuid(clientCommands,
              batchAccountAlias, "password", true);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
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
      aliases.add("testGUID" + RandomString.randomString(12));
    }
    try {
      clientCommands.guidBatchCreate(batchAccountGuid, aliases, 15 * 1000);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while creating guids: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_03_CheckBatch() {
    try {
      JSONObject accountRecord = clientCommands.lookupAccountRecord(batchAccountGuid.getGuid());
      Assert.assertEquals(numberTocreate, accountRecord.getInt("guidCnt"));
    } catch (JSONException | ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while fetching account record: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_04_BatchAccountCleanup() {
    try {
      clientCommands.accountGuidRemove(batchAccountGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing test account guid: " + e);
    }
  }
}
