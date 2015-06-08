/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor;

import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 * @param <NodeIDType>
 */
public interface EnhancedClientRequestHandlerInterface<NodeIDType> extends ClientRequestHandlerInterface<NodeIDType> {

  public NodeIDType getRandomReplica();

  public NodeIDType getRandomRCReplica();

  public NodeIDType getFirstReplica();

  public NodeIDType getFirstRCReplica();

  public void sendRequestToRandomReconfigurator(BasicReconfigurationPacket req) throws JSONException, IOException;

  public void sendRequestToReconfigurator(BasicReconfigurationPacket req, NodeIDType id) throws JSONException, IOException;

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

  /**
   * Adds a mapping between a ServiceName request and a CCPREquestID.
   * Provides backward compatibility between old Add and Remove record code and new name service code.
   *
   * @param name
   * @param id
   */
  public Object getActiveReplicaID();

}
