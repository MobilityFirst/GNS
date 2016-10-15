package edu.umass.cs.contextservice.integration;

import org.json.JSONObject;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;

/**
 * The ContextServiceGNSInterface.
 */
public interface ContextServiceGNSInterface {

  /**
   * Sends update to context service.
   *
   * @param GUID is the updated GUID in String format
   * @param attrValPairJSON JSONObject of attr:value pairs updated. attribute names
   * are the keys of JSONObject. Multiple attributes can be updated once.
   * @param versionNum version num of update, if there is none set to -1.
   * @param blocking if set to true the call will block until the update completes in CS.
   * for GNS preferably set to false.
   */
  //public void sendUpdateToCS(String GUID, JSONObject attrValPairJSON, long versionNum, boolean blocking);

  /**
   * Checks and sends trigger for the gns command.
   * Blocking if set to true indicates blocking for the context service update to complete and recv
   * the confirmation.
   * Blocking should preferably set to false, as we don't want gns to block for CS udpates.
   *
   * @param jsonFormattedCommand gns formatted command
   * @param command AbstractCommand
   * @param blocking
   */
  public void sendTiggerOnGnsCommand(JSONObject jsonFormattedCommand, AbstractCommand command, boolean blocking);
}
