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

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.config.ContextServiceConfig.PrivacySchemes;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Repeat;
import edu.umass.cs.utils.Utils;
import java.io.IOException;

import org.hamcrest.Matchers;
import org.json.JSONArray;
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
public class ContextServiceTest extends DefaultGNSTest {

  private static final int REPEAT = 10;

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;

  /**
   *
   */
  public ContextServiceTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
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
   * Test to check context service triggers.
   */
  // these two attributes right now are supported by CS
  @Test
  @Repeat(times = REPEAT)
  public void test_620_contextServiceTest() {
    // run it only when CS is enabled
    // to check if context service is enabled.
    boolean enableContextService = Config.getGlobalBoolean(GNSConfig.GNSC.ENABLE_CNS);
    String csIPPort = Config.getGlobalString(GNSConfig.GNSC.CNS_NODE_ADDRESS);
    if (enableContextService) {
      try {
        JSONObject attrValJSON = new JSONObject();
        attrValJSON.put("geoLocationCurrentLat", 42.466);
        attrValJSON.put("geoLocationCurrentLong", -72.58);

        clientCommands.update(masterGuid, attrValJSON);
        // just wait for 2 sec before sending search
        Thread.sleep(1000);

        String[] parsed = csIPPort.split(":");
        String csIP = parsed[0];
        int csPort = Integer.parseInt(parsed[1]);

        ContextServiceClient csClient = new ContextServiceClient(csIP, csPort, false, PrivacySchemes.NO_PRIVACY);

        // context service query format
        String query = "geoLocationCurrentLat >= 40 "
                + "AND geoLocationCurrentLat <= 50 AND "
                + "geoLocationCurrentLong >= -80 AND "
                + "geoLocationCurrentLong <= -70";
        JSONArray resultArray = new JSONArray();
        // third argument is arbitrary expiry time, not used now
        int resultSize = csClient.sendSearchQuery(query, resultArray,
                300000);
        Assert.assertThat(resultSize, Matchers.greaterThanOrEqualTo(1));

      } catch (Exception e) {
        Utils.failWithStackTrace("Exception during contextServiceTest: ", e);
      }
    }
  }

}
