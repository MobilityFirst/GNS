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
import edu.umass.cs.gnscommon.exceptions.client.FieldNotFoundException;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;

import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Test the fieldExists method.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FieldExistsTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands = null;
  private static GuidEntry masterGuid;
  private static GuidEntry subGuidEntry;

  /**
   *
   */
  public FieldExistsTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
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
      GuidEntry testGuid = clientCommands.guidCreate(masterGuid,
              "testGUID" + RandomString.randomString(12));
      try {
        clientCommands.guidRemove(masterGuid, testGuid.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
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
  public void test_03_FieldNotFoundException() {
    try {
      clientCommands.fieldReadArrayFirstElement(subGuidEntry.getGuid(), "environment", subGuidEntry);
      Utils.failWithStackTrace("Should have thrown an exception.");
    } catch (FieldNotFoundException e) {
      System.out.println("This was expected: " + e);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_04_FieldExistsFalse() {
    try {
      Assert.assertFalse(clientCommands.fieldExists(subGuidEntry.getGuid(), "environment", subGuidEntry));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_05_CreateField() {
    try {
      clientCommands.fieldCreateOneElementList(subGuidEntry.getGuid(), "environment", "work", subGuidEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception during create field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_06_FieldExistsTrue() {
    try {
      Assert.assertTrue(clientCommands.fieldExists(subGuidEntry.getGuid(), "environment", subGuidEntry));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_07_Cleanup() {
    try {
      clientCommands.guidRemove(masterGuid, subGuidEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing test account guid: " + e);
    }
  }
}
