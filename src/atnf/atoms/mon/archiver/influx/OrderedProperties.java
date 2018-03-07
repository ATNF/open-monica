// Copyright (C) CSIRO Australia Telescope National Facility
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.

package atnf.atoms.mon.archiver.influx;

/**
 * Created by has09e on 21/07/17.
 */
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

public class OrderedProperties {

    private static Map<String, String> properties = new LinkedHashMap<String, String>();

    private static OrderedProperties instance = null;

    private OrderedProperties() {

    }

    //The propertyFileName is read from the classpath and should be of format : key=value
    public static synchronized OrderedProperties getInstance(String propertyFileName) {
        if (instance == null) {
            instance = new OrderedProperties();
            readPropertiesFile(propertyFileName);
        }
        return instance;
    }

    private static void readPropertiesFile(String propertyFileName){
        LineIterator lineIterator = null;
        try {

            //read file from classpath
            //URL url = instance.getClass().getResource(propertyFileName);

            lineIterator = FileUtils.lineIterator(new File(propertyFileName), "UTF-8");
            while (lineIterator.hasNext()) {
                String line = lineIterator.nextLine().replace("\\\\", "\\");

                if (line.startsWith("#") ) {
                    // ignore comments
                    continue;
                }

                //Continue to parse if there are blank lines (prevents IndesOutOfBoundsException)
                if (!line.trim().isEmpty()) {
                    List<String> keyValuesPairs = Arrays.asList(line.split("="));
                    properties.put(keyValuesPairs.get(0) , keyValuesPairs.get(1));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lineIterator.close();
        }
    }

    public Map<String, String> getProperties() {
        return OrderedProperties.properties;
    }

    public String getProperty(String key) {
        return OrderedProperties.properties.get(key);
    }

}
