/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.exceptions.GnsException;
import java.io.IOException;
import org.json.JSONObject;

/**
 * The interface all the various GNS client classes should implement.
 *
 * @author westy
 */
public interface GNSClientInterface {

  /**
   * Shuts down the client.
   */
  public void stop();

  /**
   * Returns the host as a string.
   *
   * @return the host
   */
  public String getGnsRemoteHost();

  /**
   * Returns the port as an integer.
   *
   * @return the port.
   */
  public int getGnsRemotePort();

  /**
   * Returns a JSON object containing all of the guid information
   *
   * @param guid
   * @return
   * @throws IOException
   * @throws GnsException
   */
  public JSONObject lookupGuidRecord(String guid) throws IOException, GnsException;

  /**
   * Register a new account guid with the corresponding alias on the GNS server.
   * This generates a new guid and a public / private key pair. Returns a
   * GuidEntry for the new account which contains all of this information.
   *
   * @param alias - a human readable alias to the guid - usually an email
   * address
   * @param password - a required password argument used as a failsafe accessor for the account
   * @return
   * @throws Exception
   */
  public GuidEntry accountGuidCreate(String alias, String password) throws Exception;

  /**
   * Verify an account by sending the verification code back to the server.
   *
   * @param guid the account GUID to verify
   * @param code the verification code
   * @return ?
   * @throws Exception
   */
  public String accountGuidVerify(GuidEntry guid, String code) throws Exception;

  /**
   * Creates an new GUID associated with an account on the GNS server.
   *
   * @param accountGuid
   * @param alias the alias
   * @return the newly created GUID entry
   * @throws Exception
   */
  public GuidEntry guidCreate(GuidEntry accountGuid, String alias) throws Exception;

  /**
   * Creates a tag to the tags of the guid.
   *
   * @param guid
   * @param tag
   * @throws Exception
   */
  public void addTag(GuidEntry guid, String tag) throws Exception;

  /**
   * Check that the connectivity with host:port can be established
   *
   * @throws IOException throws exception if a communication error occurs
   */
  public void checkConnectivity() throws IOException;

}
