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
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Repeat;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
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
public class CreateUpdateRemoveRepeatTest extends DefaultGNSTest {

  private static final int REPEAT = 100;

  private static GNSClientCommands clientCommands = null;
  private static GuidEntry masterGuid;

  /**
   *
   */
  public CreateUpdateRemoveRepeatTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
    }
    try {
      masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   * @throws Exception
   */
  @Test
  @Repeat(times = REPEAT)
  public void test_001_CreateAndUpdate() throws Exception {
    // CHECKED FOR VALIDITY
    String alias = "testGUID" + RandomString.randomString(12);
    String createdGUID = client.execute(
            GNSCommand.guidCreate(masterGuid, alias)).getResultString();
    GuidEntry createdGUIDEntry = GuidUtils.getGUIDKeys(alias);
    Assert.assertEquals(alias, createdGUIDEntry.entityName);
    Assert.assertEquals(createdGUID, GuidUtils.getGUIDKeys(alias).guid);
    String key = "key1", value = "value1";
    client.execute(GNSCommand.update(createdGUID,
            new JSONObject().put(key, value), createdGUIDEntry));
    Assert.assertEquals(value,
            client.execute(GNSCommand.fieldRead(createdGUIDEntry, key)).getResultMap().get(key));
    client.execute(GNSCommand.guidRemove(masterGuid, createdGUID));
  }

}
