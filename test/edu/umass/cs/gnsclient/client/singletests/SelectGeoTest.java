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
import edu.umass.cs.gnsclient.client.util.JSONUtils;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.utils.RandomString;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.*;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Functionality test for core elements in the client using the UniversalGnsClientFull.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SelectGeoTest {

  private static String accountAlias = "test@cgns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static String password = "password";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;

  /**
   *
   */
  public SelectGeoTest() {
    if (client == null) {
      try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
      if (System.getProperty("alias") != null
              && !System.getProperty("alias").isEmpty()) {
        accountAlias = System.getProperty("alias");
      }
      if (System.getProperty("password") != null
              && !System.getProperty("password").isEmpty()) {
        password = System.getProperty("password");
      }
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, accountAlias, password, true);
      } catch (Exception e) {
        fail("Exception while creating account guid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_1_GeoSpatialSelect() {
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = client.guidCreate(masterGuid, "geoTest-" + RandomString.randomString(6));
        client.setLocation(testEntry, 0.0, 0.0);
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }

    try {

      JSONArray loc = new JSONArray();
      loc.put(1.0);
      loc.put(1.0);
      JSONArray result = client.selectNear(GNSProtocol.LOCATION_FIELD_NAME.toString(), loc, 2000000.0);
      // best we can do should be at least 5, but possibly more objects in results
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      fail("Exception executing selectNear: " + e);
    }

    try {

      JSONArray rect = new JSONArray();
      JSONArray upperLeft = new JSONArray();
      upperLeft.put(1.0);
      upperLeft.put(1.0);
      JSONArray lowerRight = new JSONArray();
      lowerRight.put(-1.0);
      lowerRight.put(-1.0);
      rect.put(upperLeft);
      rect.put(lowerRight);
      JSONArray result = client.selectWithin(GNSProtocol.LOCATION_FIELD_NAME.toString(), rect);
      // best we can do should be at least 5, but possibly more objects in results
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      fail("Exception executing selectWithin: " + e);
    }
  }

}
