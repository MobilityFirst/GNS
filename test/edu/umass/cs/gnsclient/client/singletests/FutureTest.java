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

import edu.umass.cs.gigapaxos.interfaces.RequestFuture;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.packets.CommandPacket;

import edu.umass.cs.gnsserver.gnsapp.GNSCommandInternal;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.json.JSONException;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Testing futures.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FutureTest extends DefaultGNSTest {

  private static GuidEntry masterGuid;

  /**
   *
   */
  public FutureTest() {
    try {
      masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
      System.out.println(masterGuid.getEntityName() + " " + masterGuid.getGuid());
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while getting master key: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_01_FutureTest() {

    // Create a field that we can remove
    try {
      client.execute(GNSCommand.fieldAppendOrCreate(masterGuid.getGuid(), "fred", "value", masterGuid));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while creating field: " + e);
    }

    // Use an async call to remove the field
    RequestFuture<CommandPacket> future = null;
    try {
      future = client.executeAsync(GNSCommandInternal.fieldRemove(masterGuid.getGuid(), "fred", "value",
              getTestHeader(4, "randomGUID", 312312312)));
    } catch (InternalRequestException | JSONException | IOException e) {
      Utils.failWithStackTrace("Exception while creating command packet: " + e);
    }

    // Get the result from the future
    if (future != null) {
      try {
        System.out.println("FUTURE:" + future);
        System.out.println("DONE:" + future.isDone());
        System.out.println("PACKET:" + future.get());
        System.out.println("RESULT:" + future.get().getResultString());
      } catch (ExecutionException | InterruptedException | ClientException e) {
        Utils.failWithStackTrace("Exception while waiting for response: " + e);
      }
    }
  }

  // Copied from InternalCommandPacket test code
  private static InternalRequestHeader getTestHeader(int ttl, String GUID,
          long qid) {
    return new InternalRequestHeader() {

      @Override
      public long getOriginatingRequestID() {
        return qid;
      }

      @Override
      public String getOriginatingGUID() {
        return GUID;
      }

      @Override
      public int getTTL() {
        return ttl;
      }

      @Override
      public boolean hasBeenCoordinatedOnce() {
        return false;
      }
    };
  }
}
