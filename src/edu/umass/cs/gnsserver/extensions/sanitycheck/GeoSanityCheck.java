/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Karthik A.
 *
 */

package edu.umass.cs.gnsserver.extensions.sanitycheck;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/* This is an implementation of sanity checker */
public class GeoSanityCheck extends AbstractSanityCheck {

    @Override
    public void check(Request request) throws RequestParseException {

        try {
            JSONObject requestJSON = new JSONObject(request.toString());
            JSONObject commandQuery = requestJSON.getJSONObject(GNSProtocol.COMMAND_QUERY.toString());

            if (commandQuery.has(GNSProtocol.COMMAND_INT.toString()) &&
                    commandQuery.getInt(GNSProtocol.COMMAND_INT.toString()) == CommandType.ReplaceUserJSON.getInt()) {
                JSONObject userJSON = new JSONObject(commandQuery.getString(GNSProtocol.USER_JSON.toString()));
                if (userJSON.has(GNSProtocol.LOCATION_FIELD_NAME_2D_SPHERE.toString())) {
                    JSONObject geoLocCurrent = userJSON.getJSONObject(GNSProtocol.LOCATION_FIELD_NAME_2D_SPHERE.toString());
                    if (geoLocCurrent.has("coordinates")) {
                        JSONArray coordinates = geoLocCurrent.getJSONArray("coordinates");
                        if(coordinates.get(0).getClass().equals(String.class) || coordinates.get(1).getClass().equals(String.class)) {
                            throw new RequestParseException(new Exception("Numeric value expected for location coordinates, string provided"));
                        }
                    }
                }
            }

        } catch (JSONException e) {
            //Pass
        }
    }
}
