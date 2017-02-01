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
package edu.umass.cs.gnsclient.client.singletests.failingtests;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GUIDUtilsHTTPClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;

import org.junit.Test;

/**
 * This tests writes bigger field values until it fails.
 * 
 * @author westy
 */
public class WriteSizeTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;

  /**
   *
   */
  public WriteSizeTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
      try {
        masterGuid = GUIDUtilsHTTPClient.getGUIDKeys(globalAccountName);
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void writeSizeTest() {
    int numValues = 100;
    int valueSizeIncrement = 50000;
    GuidEntry tempEntry = null;
    System.out.println("Writing values to field with sizes from " + valueSizeIncrement + " to " + valueSizeIncrement * 100
            + " by increments of " + valueSizeIncrement);
    try {
      String guidName = "testGUID" + RandomString.randomString(20);
      System.out.println("Creating guid: " + guidName);
      tempEntry = clientCommands.guidCreate(masterGuid, guidName);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception creating guid: " + e);
    }
    if (tempEntry != null) {
      String fieldName = "testField" + RandomString.randomString(10);
      String fieldValue = RandomString.randomString(10);
      System.out.println("Creating field: " + fieldName);
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
      for (int k = 0; k < numValues - 1; k++) {
        String value = RandomString.randomString(valueSizeIncrement * k + 1);
        System.out.println("Writing value of length " + value.length());
        try {
          clientCommands.fieldReplace(tempEntry, fieldName, value);
        } catch (IOException | ClientException e) {
          Utils.failWithStackTrace("Exception appending value onto field: " + e);
        }
        try {
          clientCommands.fieldReadArray(tempEntry.getGuid(), fieldName, tempEntry);
        } catch (ClientException | IOException e) {
          Utils.failWithStackTrace("Exception appending value onto field: " + e);
        }
      }
      try {
        clientCommands.guidRemove(masterGuid, tempEntry.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while deleting guid: " + e);
      }
    }
  }
}
