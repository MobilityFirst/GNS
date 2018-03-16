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
package edu.umass.cs.gnsclient.client.singletests.simple;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;

import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Test reads.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SingleReadTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands = null;
  private static GuidEntry masterGuid;
  private static GuidEntry subGuidEntry;

  /**
   *
   */
  public SingleReadTest() {
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
  public void test_01_CreateEntity() {
    try {
      GuidEntry testEntry = clientCommands.guidCreate(masterGuid, "testGUID" + RandomString.randomString(12));
      try {
        clientCommands.guidRemove(masterGuid, testEntry.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception during cleanup: " + e);
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_02_CreateSubGuid() {
    try {
      subGuidEntry = clientCommands.guidCreate(masterGuid, "subGuid" + RandomString.randomString(12));
      System.out.println("Created: " + subGuidEntry);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_03_CreateField() {
    try {
      clientCommands.fieldUpdate(subGuidEntry.getGuid(), "environment", "work", subGuidEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception during create field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_04_ReadField() {
    try {
      // read my own field
      Assert.assertEquals("work", clientCommands.fieldRead(subGuidEntry.getGuid(), "environment", subGuidEntry));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception reading field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_05_Cleanup() {
    try {
      clientCommands.guidRemove(masterGuid, subGuidEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception during cleanup: " + e);
    }
  }
}
