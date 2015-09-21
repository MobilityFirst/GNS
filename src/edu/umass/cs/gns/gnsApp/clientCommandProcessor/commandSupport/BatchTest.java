/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;

/**
 * 
 * This class contains the code for the batch test.
 *
 * @author westy
 */
public class BatchTest {

  private static final String ACCOUNTNAME = "BatchTest";
  private static final String PUBLICKEY = "BATCH";

  /**
   * This method implements the Batch test. 
   * 
   * @param guidCnt
   * @param handler 
   */
  public static void runBatchTest(int guidCnt, ClientRequestHandlerInterface handler) {
    String accountGuid;
    // see if we already registered our GUID
    if ((accountGuid = AccountAccess.lookupGuid(ACCOUNTNAME, handler)) == null) {
      // if not we use the method  below which bypasses the normal email verification requirement
      // but first we create a GUID from our public key
      accountGuid = ClientUtils.createGuidStringFromPublicKey(PUBLICKEY.getBytes());
      AccountAccess.addAccount(ACCOUNTNAME, accountGuid, PUBLICKEY, "", false, null, handler);
    }
    
    AccountAccess.testBatchCreateGuids(
            AccountAccess.lookupAccountInfoFromGuid(accountGuid, handler), 
            AccountAccess.lookupGuidInfo(accountGuid, handler),
            guidCnt, handler); 
  }
}
