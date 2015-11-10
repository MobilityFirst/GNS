/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gnsserver.utils;

import java.io.UnsupportedEncodingException;
import java.util.Collection;

/**
 * Parser for URL query strings.
 */
public class URLQueryStringParser {
    private final String queryString;
    
    /**
     * The position of the current parameter.
     */
    private int paramBegin;
    private int paramEnd = -1;
    private int paramNameEnd;
    private String paramName;
    private String paramValue;

    /**
     * Construct a parser from the given URL query string.
     * 
     * @param queryString the query string, i.e. the part of the URL starting
     *                    after the '?' character
     */
    public URLQueryStringParser(String queryString) {
        this.queryString = queryString;
    }
    
    /**
     * Move to the next parameter in the query string.
     * 
     * @return <code>true</code> if a parameter has been found; 
     * <code>false</code> if there are no more parameters
     */
    public boolean next() {
        int len = queryString.length();
        while (true) {
            if (paramEnd == len) {
                return false;
            }
            paramBegin = paramEnd == -1 ? 0 : paramEnd+1;
            int idx = queryString.indexOf('&', paramBegin);
            paramEnd = idx == -1 ? len : idx;
            if (paramEnd > paramBegin) {
                idx = queryString.indexOf('=', paramBegin);
                paramNameEnd = idx == -1 || idx > paramEnd ? paramEnd : idx;
                paramName = null;
                paramValue = null;
                return true;
            }
        }
    }
    
    /**
     * Search for a parameter with a name in a given collection.
     * This method iterates over the parameters until a parameter with
     * a matching name has been found. Note that the current parameter is not
     * considered.
     * 
     * @param names
     * @return true if the parameter is found
     */
    public boolean search(Collection<String> names) {
        while (next()) {
            if (names.contains(getName())) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Get the name of the current parameter.
     * Calling this method is only allowed if {@link #next()} has been called
     * previously and the result of this call was <code>true</code>. Otherwise the
     * result of this method is undefined.
     * 
     * @return the name of the current parameter
     */
    public String getName() {
        if (paramName == null) {
            paramName = queryString.substring(paramBegin, paramNameEnd);
        }
        return paramName;
    }
    
    /**
     * Get the value of the current parameter.
     * Calling this method is only allowed if {@link #next()} has been called
     * previously and the result of this call was <code>true</code>. Otherwise the
     * result of this method is undefined.
     * 
     * @return the decoded value of the current parameter
     */
    public String getValue() {
        if (paramValue == null) {
            if (paramNameEnd == paramEnd) {
                return null;
            }
            try {
                paramValue = URIEncoderDecoder.decode(queryString.substring(paramNameEnd+1, paramEnd));
            } catch (UnsupportedEncodingException ex) {
                // TODO: URIEncoderDecoder.decode should be changed to not throw
                // UnsupportedEncodingException since this actually never happens (the charset
                // being looked up is UTF-8 which always exists).
                throw new Error(ex);
            }
        }
        return paramValue;
    }
}
