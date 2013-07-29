package edu.umass.cs.gns.nio;

import java.util.ArrayList;

import edu.umass.cs.gns.util.ConfigFileInfo;

public abstract class PacketDemultiplexer {
	
	
	public abstract void handleJSONObjects(ArrayList jsonObjects);
}
