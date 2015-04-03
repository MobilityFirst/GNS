package edu.umass.cs.gns.activecode.protocol;

import java.io.Serializable;

import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;

public class ActiveCodeMessage implements Serializable {
	public boolean shutdown;
	public boolean finished;
	
	public ActiveCodeParams acp;
	public String valuesMapString;
	
	public ActiveCodeQueryRequest acqreq;
	public ActiveCodeQueryResponse acqresp;
}
