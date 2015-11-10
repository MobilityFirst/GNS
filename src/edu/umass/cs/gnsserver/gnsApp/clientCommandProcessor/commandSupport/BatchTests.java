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

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;

/**
 * 
 * This class contains the code for the batch test which tests the code
 * which creates multiple guids.
 *
 * @author westy
 */
public class BatchTests {

  /** */
  public static final String DEFAULT_ACCOUNTNAME = "BatchTest";
  /** */
  public static final String DEFAULT_PUBLICKEY = "BATCH";

  /**
   * This method implements the Batch test using the default account name and public key.
   * 
   * @param guidCnt
   * @param handler 
   */
  public static void runBatchTest(int guidCnt, ClientRequestHandlerInterface handler) {
    runBatchTest(DEFAULT_ACCOUNTNAME, DEFAULT_PUBLICKEY, guidCnt, handler);
  }
  
  /**
   * This method implements the Batch test.
   * 
   * @param accountName
   * @param publicKey
   * @param guidCnt
   * @param handler 
   */
  public static void runBatchTest(String accountName, String publicKey, int guidCnt,
          ClientRequestHandlerInterface handler) {
    String accountGuid;
    // see if we already registered our GUID
    if ((accountGuid = AccountAccess.lookupGuid(accountName, handler)) == null) {
      // if not we use the method  below which bypasses the normal email verification requirement
      // but first we create a GUID from our public key
      accountGuid = ClientUtils.createGuidStringFromPublicKey(publicKey.getBytes());
      AccountAccess.addAccount(accountName, accountGuid, publicKey, "", false, null, handler);
    }
    
    AccountAccess.testBatchCreateGuids(
            AccountAccess.lookupAccountInfoFromGuid(accountGuid, handler), 
            AccountAccess.lookupGuidInfo(accountGuid, handler),
            guidCnt, handler); 
  }
}
