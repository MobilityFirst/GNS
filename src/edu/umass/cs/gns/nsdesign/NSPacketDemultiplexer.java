package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nio.PacketDemultiplexer;
import org.json.JSONObject;

import java.util.ArrayList;

/**
 * Work in progress. Inactive code.
 *
 * Forwards incoming json objects to name server which
 * decides whether to send to active replica or replica controller.
 * Created by abhigyan on 2/26/14.
 */
public class NSPacketDemultiplexer extends PacketDemultiplexer{

  NameServer nameServerInterface;

  public NSPacketDemultiplexer(NameServer nameServerInterface) {
    this.nameServerInterface = nameServerInterface;
  }

  @Override
  public void handleJSONObjects(ArrayList<JSONObject> jsonObjects) {
    for (JSONObject json: jsonObjects)
      nameServerInterface.handleIncomingPacket(json);
  }
}
