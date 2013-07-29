package nio;

import java.util.ArrayList;

import edu.umass.cs.gnrs.util.ConfigFileInfo;

public abstract class PacketDemultiplexer {
	
	
	public abstract void handleJSONObjects(ArrayList jsonObjects);
}
