/**
 * Mobility First - Global Name Resolution Service (GNS)
 * Copyright (C) 2013 University of Massachusetts - Emmanuel Cecchet.
 * Contact: cecchet@cs.umass.edu
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package edu.umass.cs.msocket.contextsocket;

import java.util.Iterator;
import java.util.Map;

import edu.umass.cs.msocket.gns.GNSCalls;
import edu.umass.cs.msocket.gns.GnsIntegration;

/**
 * This class defines a MSocketGroupMember
 * This class is used to set the context attributes and
 * receive any messages that are sent with context information matching to this MSocketGroupMember
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ContextMember
{
	//private String groupName ="";
	private final String localName ;
	private ContextMemberInternals msocketGroupMemberInternalsObj = null;
	
	/**
	 * @param groupName: multicast groupName
	 * @throws Exception
	 */
	public ContextMember(String localName, Map<String, Object> contextMap) throws Exception
	{
		this.localName = localName;
		msocketGroupMemberInternalsObj = new ContextMemberInternals(localName);
		
		Iterator<String> setIter = contextMap.keySet().iterator();
		while( setIter.hasNext() )
		{
			String attrName = setIter.next();
			Object value = contextMap.get(attrName);
			setAttributes(attrName, (double)value);
		}
		//setLocation(100, 100);
	}
	
	public ContextSocket accept()
	{
		return msocketGroupMemberInternalsObj.acceptNewWriter();
	}
	
	public String getMyGUID()
	{
		try
		{
			return GnsIntegration.getGUIDOfAlias(localName);
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return "";
	}
	
	public void setAttributes(String field, double value)
	{
		if( ContextSocketConfig.USE_GNS )
		{
			try
			{
				// FIXME: replace this with message to context service
				GNSCalls.writeKeyValue(localName, field, value);
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		else
		{
			ContextServiceCallsSingleton.sendUpdateToContextService(msocketGroupMemberInternalsObj.getMyGUID(), 
					field, value, msocketGroupMemberInternalsObj);
		}
	}
	
	public void close()
	{
		msocketGroupMemberInternalsObj.close();
	}
}