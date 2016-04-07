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

/**
 * Contains configuration parameters, which turn on or off some features.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ContextSocketConfig 
{
	// indicates usage of GNS to send the value update, if set to
	// false value update is directly sent o CS.
	public static final boolean USE_GNS					= false;
	
	
	// if set to true, a thread in ContextServiceSingleton periodically
	// checks for refreshes in any group.
	public static final boolean PERIODIC_GROUP_UPDATE	= true;
	
	// per 1 sec
	public static final int	GROUP_UPDATE_DELAY			= 1000;
	
	//FIXME: need some good to determine this info. or some global address like GNS
	public static final String contextNodeIP			= "127.0.0.1";
	public static final int contextNodePort				= 5000;
	
}