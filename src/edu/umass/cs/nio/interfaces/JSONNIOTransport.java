/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.nio.interfaces;

import org.json.JSONObject;

/**
 * @author V. Arun
 * 
 *         This interface was useful primarily for backwards compatibility and
 *         is deprecated now. Use just {@link Messenger
 *         InterfaceMessenger} or, if really needed,
 *         {@link InterfaceNIOTransport
 *         InterfaceNIOTransport<NodeIDType,JSONObject}instead.
 * 
 * @param <NodeIDType>
 */
@Deprecated
public interface JSONNIOTransport<NodeIDType> extends
		InterfaceNIOTransport<NodeIDType, JSONObject> {
}
