/* Copyright (1c) 2016 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (1the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 */
package edu.umass.cs.gnsclient.client;

/**
 * The result types that can be returned by executing {@link edu.umass.cs.gnscommon.packets.CommandPacket}.
 */
public enum CommandResultType {
  /**
   * The default methods {@link edu.umass.cs.gnscommon.packets.CommandPacket#getResultString()} or
   * {@link edu.umass.cs.gnscommon.packets.CommandPacket#getResult()} be used to retrieve the result
   * irrespective of the result type.
   */
  STRING, /**
   * The methods {@link edu.umass.cs.gnscommon.packets.CommandPacket#getResultMap} or
   * {@link edu.umass.cs.gnscommon.packets.CommandPacket#getResultJSONObject} can be used if and only if
   * the result type is {@link #MAP};
   */
  MAP, /**
   * The methods {@link edu.umass.cs.gnscommon.packets.CommandPacket#getResultList},
   * {@link edu.umass.cs.gnscommon.packets.CommandPacket#getResultJSONArray} can be used if and only if
   * the result type is {@link #LIST}
   */
  LIST, /**
   * The method {@link edu.umass.cs.gnscommon.packets.CommandPacket#getResultBoolean} can be used if and
   * only if the result type is {@link #BOOLEAN}.
   */
  BOOLEAN, /**
   * The method {@link edu.umass.cs.gnscommon.packets.CommandPacket#getResultLong} can be used if and
   * only if the result type is {@link #LONG}.
   */
  LONG, /**
   * The method {@link edu.umass.cs.gnscommon.packets.CommandPacket#getResultInt} can be used if and only
   * if the result type is {@link #INTEGER}.
   */
  INTEGER, /**
   * The method {@link edu.umass.cs.gnscommon.packets.CommandPacket#getResultDouble} can be used if and
   * only if the result type is {@link #DOUBLE}.
   */
  DOUBLE, /**
   * The result of executing this command is null or does not return a
   * result.
   */
  NULL

}
