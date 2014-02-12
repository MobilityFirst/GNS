package edu.umass.cs.gns.nio;

import java.util.ArrayList;
import org.json.JSONObject;

public abstract class PacketDemultiplexer {
	public abstract void handleJSONObjects(ArrayList<JSONObject> jsonObjects);
}
