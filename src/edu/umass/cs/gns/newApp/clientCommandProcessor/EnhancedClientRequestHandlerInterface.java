/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor;

import edu.umass.cs.gns.newApp.NewApp;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Extensions of ClientRequestHandlerInterface for the new app.
 * 
 * @author westy
 */
public interface EnhancedClientRequestHandlerInterface extends ClientRequestHandlerInterface {

  public String getRandomReplica();

  public String getRandomRCReplica();

  public String getFirstReplica();

  public String getFirstRCReplica();

  public void sendRequestToRandomReconfigurator(BasicReconfigurationPacket req) throws JSONException, IOException;

  public void sendRequestToReconfigurator(BasicReconfigurationPacket req, String id) throws JSONException, IOException;

  public boolean handleEvent(JSONObject json) throws JSONException;

  /**
   * Adds a mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @param id
   */
  public void addCreateRequestNameToIDMapping(String name, int id);

  /**
   * Looks up the mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @return
   */
  public Integer getCreateRequestNameToIDMapping(String name);

  /**
   * Removes the mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @return the request if or null if if can't be found
   */
  public Integer removeCreateRequestNameToIDMapping(String name);

  /**
   * Adds a mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @param id
   */
  public void addDeleteRequestNameToIDMapping(String name, int id);

  /**
   * Looks up the mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @return
   */
  public Integer getDeleteRequestNameToIDMapping(String name);

  /**
   * Removes the mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @return the request if or null if if can't be found
   */
  public Integer removeDeleteRequestNameToIDMapping(String name);

  /**
   * Adds a mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @param id
   */
  public void addActivesRequestNameToIDMapping(String name, int id);

  /**
   * Looks up the mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @return
   */
  public Integer getActivesRequestNameToIDMapping(String name);

  /**
   * Removes the mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @return the request if or null if if can't be found
   */
  public Integer removeActivesRequestNameToIDMapping(String name);

  @Override
  public String getActiveReplicaID();

  public NewApp getApp();
  
}
