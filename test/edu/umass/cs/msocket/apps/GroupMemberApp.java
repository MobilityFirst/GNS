package edu.umass.cs.msocket.apps;

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


public class GroupMemberApp 
{	
	/*public static void main(String[] args) throws Exception {
		String localName = args[0];
		
		ContextMember groupMember =  new ContextMember(localName);
		
		groupMember.setAttributes("contextATT0", 50);
		
		ContextSocketInputStream min = (ContextSocketInputStream) groupMember.getInputStream();
		System.out.println("group Member working");
		
		while(true) {
			
			byte[] b =  min.readAny();
			if(b!=null)
			{
				System.out.println(new String(b));
			}
			Thread.sleep(2000);
		}
	}*/
}
