/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
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
