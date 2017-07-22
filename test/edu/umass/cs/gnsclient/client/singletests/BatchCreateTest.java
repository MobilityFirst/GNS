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
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Repeat;
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
public class BatchCreateTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;

  /**
   *
   */
  public BatchCreateTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
    }
  }

  private static int numberToCreate = 2;

  /**
   * The test set for testing batch creates. The test will create batches
   * of size 2, 4, 8,..., 128.
   *
   * @throws Exception
   */
  @Test
  // Fixme: put this back at 7 once removal is faster.
  @Repeat(times = 5)
  public void test_500_Batch_Tests() throws Exception {
    GuidEntry accountGuidForBatch = test_510_CreateBatchAccountGuid();
    test_511_CreateBatch(accountGuidForBatch);
    test_512_CheckBatch(accountGuidForBatch);
    numberToCreate *= 2;
    client.execute(GNSCommand.accountGuidRemove(accountGuidForBatch));
  }

  /**
   * Create an account for batch test.
   *
   * @return GuidEntry
   * @throws Exception
   */
  public GuidEntry test_510_CreateBatchAccountGuid() throws Exception {
    //CHECKED FOR VALIDITY
    // can change the number to create on the command line
//    GuidEntry accountGuidForBatch;
    if (System.getProperty("count") != null
            && !System.getProperty("count").isEmpty()) {
      numberToCreate = Integer.parseInt(System.getProperty("count"));
    }
    String batchAccountAlias = "batchTest510"
            + RandomString.randomString(12) + "@gns.name";
    client.execute(GNSCommand.createAccount(batchAccountAlias));
    return GuidUtils.getGUIDKeys(batchAccountAlias);
//    accountGuidForBatch = GuidUtils.lookupOrCreateAccountGuid(clientCommands,
//            batchAccountAlias, "password", true);
//    return accountGuidForBatch;
  }

  /**
   * Create some guids with batch create.
   *
   * @param accountGuidForBatch
   */
  public void test_511_CreateBatch(GuidEntry accountGuidForBatch) {
    //CHECKED FOR VALIDITY
    Set<String> aliases = new HashSet<>();
    for (int i = 0; i < numberToCreate; i++) {
      //Brendan: I added Integer.toString(i) to this to guarantee no collisions during creation.
      aliases.add("testGUID511" + Integer.toString(i) + RandomString.randomString(12));
    }
    try {
      clientCommands.guidBatchCreate(accountGuidForBatch, aliases, 20 * 1000);
      //result = client.guidBatchCreate(accountGuidForBatch, aliases);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while creating guids: ", e);
    }
    //Assert.assertEquals(GNSProtocol.OK_RESPONSE.toString(), result);
  }

  /**
   * Check the batch creation.
   *
   * @param accountGuidForBatch
   */
  public void test_512_CheckBatch(GuidEntry accountGuidForBatch) {
    //CHECKED FOR VALIDITY
    try {
      JSONObject accountRecord = clientCommands
              .lookupAccountRecord(accountGuidForBatch.getGuid());
      Assert.assertEquals(numberToCreate, accountRecord.getInt("guidCnt"));
    } catch (JSONException | ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while fetching account record: ", e);
    }
  }

}
