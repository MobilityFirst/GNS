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
package edu.umass.cs.gnsclient.client.singletests.performance;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;

import org.json.JSONArray;

import org.junit.Test;

/**
 *
 * @author westy
 */
public class PerformanceTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;

  /**
   *
   */
  public PerformanceTest() {
    if (clientCommands == null) {
        clientCommands = new GNSClientCommands(client);
        clientCommands.setForceCoordinatedReads(true);
      
      try {
        masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
      }
    }
  }

  /**
   *
   */
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
        tempEntry = clientCommands.guidCreate(masterGuid, guidName);
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception creating guid: " + e);
      }

      if (tempEntry != null) {
        for (int j = 0; j < numFields; j++) {
          String fieldName = RandomString.randomString(10);
          String fieldValue = RandomString.randomString(10);
          System.out.println("Creating field #" + j + " : " + fieldName);
          try {
            clientCommands.fieldCreateOneElementList(tempEntry, fieldName, fieldValue);
          } catch (IOException | ClientException e) {
            Utils.failWithStackTrace("Exception creating field: " + e);
          }
          try {
            clientCommands.fieldReadArray(tempEntry.getGuid(), fieldName, tempEntry);
          } catch (ClientException | IOException e) {
            Utils.failWithStackTrace("Exception reading field: " + e);
          }
          ArrayList<String> allValues = new ArrayList<>();
          for (int k = 0; k < numValues; k++) {
            allValues.add(RandomString.randomString(valueSizeIncrement));
          }
          try {
            clientCommands.fieldAppend(tempEntry.getGuid(), fieldName, new JSONArray(allValues), tempEntry);
          } catch (IOException | ClientException e) {
            Utils.failWithStackTrace("Exception appending value onto field: " + e);
          }
          JSONArray array;
          try {
            array = clientCommands.fieldReadArray(tempEntry.getGuid(), fieldName, tempEntry);
            System.out.println("Length of array = " + array.toString().length());
          } catch (ClientException | IOException e) {
            Utils.failWithStackTrace("Exception appending value onto field: " + e);
          }
        }
        System.out.println("Done with guid #" + i);
        try {
          clientCommands.guidRemove(masterGuid, tempEntry.getGuid());
        } catch (ClientException | IOException e) {
          Utils.failWithStackTrace("Exception while deleting guid: " + e);
        }
      }
    }
  }
}
