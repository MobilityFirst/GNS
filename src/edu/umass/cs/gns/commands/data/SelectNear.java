/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.data;

import edu.umass.cs.gns.commands.GnsCommand;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.clientsupport.FieldAccess;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class SelectNear extends GnsCommand {

  public SelectNear(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{FIELD, NEAR, MAXDISTANCE};
  }

  @Override
  public String getCommandName() {
    return SELECT;
  }

  @Override
  public String execute(JSONObject json) throws JSONException {
    String field = json.getString(FIELD);
    String value = json.getString(NEAR);
    String maxDistance = json.getString(MAXDISTANCE);
    return FieldAccess.selectNear(field, value, maxDistance);
  }

  @Override
  public String getCommandDescription() {
    return "Key must be a GeoSpatial field. Return all fields that are within max distance of value."
            + "Value is a point specified as a JSONArray string tuple: [LONG, LAT]. Max Distance is in meters.";
  }
}
