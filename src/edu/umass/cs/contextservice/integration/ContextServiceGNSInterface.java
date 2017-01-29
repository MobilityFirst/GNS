package edu.umass.cs.contextservice.integration;

import org.json.JSONObject;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;


public interface ContextServiceGNSInterface {


  //public void sendUpdateToCS(String GUID, JSONObject attrValPairJSON, long versionNum, boolean blocking);


  public void sendTiggerOnGnsCommand(JSONObject jsonFormattedCommand, AbstractCommand command, boolean blocking);
}
