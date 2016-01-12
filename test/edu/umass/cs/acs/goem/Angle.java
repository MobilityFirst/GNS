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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.acs.goem;

import static java.lang.Math.*;

/**
 * Utility methods for dealing with angles.
 *  
 */
public class Angle
{
   /**
    * Disallow instantiation.
    */
   private Angle()
   {
   }

    public static double normalizeDegrees(double angle) {
	return angle < 0 ? angle + 360 : 
	    (angle < 360 ? angle : angle - 360);
    }

   /**
    * A wrapper for atan2 that returns positive degrees
    */
    public static double getAngleInDegrees(double dx, double dy) {
        double phi = - atan2( dy , dx );
        // make sure it is positive
        if( phi < 0 )
            phi = 2 * PI + phi;
        return phi * 180.0/PI;
    }
    
   /**
    * returns the difference between two angles specified in degrees in positive degrees
    */
    public static double differenceInDegrees( double from, double to){
	//compute difference
	double dangle = to - from;
	// account for the ever confusing 360 redundancy
	while( dangle < 0 )
	    dangle += 360;
	while( dangle > 360)
	    dangle -= 360;
	return dangle;
    }

    /**
    * returns the difference between two angles specified in degrees in in positive degrees normalized to 180 degrees
    */
    public static double differenceInDegrees180( double from, double to){
	//compute difference
	double dangle = to - from;
	// account for the ever confusing 360 redundancy
	while( dangle < 0 )
	    dangle += 180;
	while( dangle > 180)
	    dangle -= 180;
	return dangle;
    }
}
