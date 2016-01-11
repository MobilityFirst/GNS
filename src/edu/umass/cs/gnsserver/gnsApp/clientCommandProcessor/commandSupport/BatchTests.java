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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

import static edu.umass.cs.gnscommon.GnsProtocol.BAD_GUID;
import static edu.umass.cs.gnscommon.GnsProtocol.BAD_RESPONSE;
import static edu.umass.cs.gnscommon.GnsProtocol.OK_RESPONSE;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;

/**
 * 
 * This class contains the code for the batch test which tests the code
 * which creates multiple guids.
 *
 * @author westy
 */
public class BatchTests {

  /**
   * This method implements the Batch test.
   * 
   * @param accountName
   * @param guidCnt
   * @param handler 
   * @return a command response object
   */
  public static CommandResponse<String> runBatchTest(String accountName, int guidCnt,
          ClientRequestHandlerInterface handler) {
    String accountGuid;
    // see if we already registered our GUID
    if ((accountGuid = AccountAccess.lookupGuid(accountName, handler)) == null) {
       return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_GUID + " " + accountGuid);
    }
    AccountAccess.testBatchCreateGuids(
            AccountAccess.lookupAccountInfoFromGuid(accountGuid, handler), 
            AccountAccess.lookupGuidInfo(accountGuid, handler),
            guidCnt, handler); 
    
    return new CommandResponse<>(OK_RESPONSE);
  }
}
