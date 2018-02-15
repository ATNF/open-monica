package atnf.atoms.mon.archiver;

import atnf.atoms.mon.PointData;
import atnf.atoms.mon.PointDescription;
import atnf.atoms.mon.util.MonitorConfig;
import atnf.atoms.time.AbsTime;
import atnf.atoms.time.RelTime;
import au.csiro.pbdava.diamonica.etl.influx.TagExtractor;
import au.csiro.pbdava.diamonica.etl.influx.OrderedProperties;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.util.Vector;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;
import org.influxdb.InfluxDB.LogLevel;
import org.apache.log4j.Logger;

// todo worker thread for save now
// put all config options in config file



/**
 * #%L
 * CSIRO VO Tools
 * %%
 * Copyright (C) 2010 - 2016 Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 *
 *
 * Licensed under the CSIRO Open Source License Agreement (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file.
 * #L%
 */

public class PointArchiverInfluxDb extends PointArchiver{

  /**
   *  InfluxDB port number (default)
   */
  private final static int port = 8089;

  /**
   * InfluxDB Diamonica database name
   */
  private static String databaseName = "askap_realtime";

  /**
   * ASKAP site prefix, needed for matching atennas vs blocks
   * but what about other sites
   */
  private static String site = "rw";

  /**
   * Global retention policy
   */
  private static String retentionPolicy = "autogen";

  /**
   * The connection to the InfluxDB server.
   */
  private InfluxDB influxDB = null;


  /**
   * The URL to connect to the server/database.
   */
  private static String itsURL = "http://localhost:8086";

  /**
   * Tag file location
   */
  private static String tagFilePath = "/home/has09e/etc/monica/rw.properties";
  
  protected static long theirInfluxBatchSize = 1000;
  protected static long theirInfluxBatchAge = 1000;

  private Map<String, TagExtractor> tags = null;
  /**
   * Maximum chunk size
   */
  private int chunkSize = 5000;

  /**
   * Total size
   */
  private long totalSize = 0L;

  /**
   * Batch points object, built with
   */
  private final BatchPoints batchPoints = BatchPoints
          .database(databaseName)
          .retentionPolicy(retentionPolicy)
          .consistency(InfluxDB.ConsistencyLevel.ALL)
          .build();

  private long itsOldestPointTime = 0;
  private long itsNewestPointTime = 0;
  private long itsLastWriteTime = 0;

  /** Static block to parse flush parameters. */
  static {
    try {
      String param= MonitorConfig.getProperty("InfluxURL", "http://localhost:8086");
      itsURL = param;
    } catch (Exception e) {
      Logger.getLogger(PointArchiver.class.getName()).warn("Error parsing InfluxBatchSize configuration parameter: " + e);
    }
    try {
      String param= MonitorConfig.getProperty("InfluxTagMap");
      tagFilePath = param;
    } catch (Exception e) {
      Logger.getLogger(PointArchiver.class.getName()).warn("Error parsing InfluxTagMap configuration parameter: " + e);
    }
    try {
      long param = Integer.parseInt(MonitorConfig.getProperty("InfluxBatchSize", "1000"));
      theirInfluxBatchSize = param;
    } catch (Exception e) {
      Logger.getLogger(PointArchiver.class.getName()).warn("Error parsing InfluxBatchSize configuration parameter: " + e);
    }
    
    try {
      long param = 1000 * Integer.parseInt(MonitorConfig.getProperty("InfluxBatchAge", "1"));
      theirInfluxBatchAge = param;
    } catch (Exception e) {
      Logger.getLogger(PointArchiver.class.getName()).warn("Error parsing InfluxBatchAge configuration parameter: " + e);
    }
  }
  
  /**
   * Create a influxDB connection
   *
   * @throws InterruptedException
   * @throws IOException
   */
  private void setUp() throws InterruptedException, IOException {
    this.influxDB = InfluxDBFactory.connect(itsURL, "admin", "admin");
    boolean influxDBstarted = false;
    do {
      Pong response;
      try {
        response = this.influxDB.ping();
        if (!response.getVersion().equalsIgnoreCase("unknown")) {
          influxDBstarted = true;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      Thread.sleep(100L);
    } while (!influxDBstarted);
    this.influxDB.setLogLevel(LogLevel.NONE);
    System.out.println("################################################################################## ");
    System.out.println("#  Connected to InfluxDB Version: " + this.influxDB.version() + " #");
    System.out.println("##################################################################################");

    // load tags
    final OrderedProperties tagProperties = OrderedProperties.getInstance(tagFilePath);
    tags = new LinkedHashMap<String, TagExtractor>();
    tagProperties.getProperties().forEach((k,v) -> tags.put(k, TagExtractor.fromString(v.replace("$(site)", site))));
  }

  /**
   * Check if we are connected to the server and reconnect if required.
   *
   * @return True if connected (or reconnected). False if not connected.
   */
  protected boolean isConnected() {
    return influxDB != null;
  }

  /**
   * Ping influxdb
   *
   * @print The version and response time of the influxDB object's database
   */
  protected void Ping() {
    Pong result = this.influxDB.ping();
    String version = result.getVersion();
    System.out.println("Version: " + version + " | Response: " + result + "ms");
    System.out.println(influxDB.describeDatabases());
  }

  /**
   * Extract data from the archive with no undersampling.
   *
   * @param pm
   *          Point to extract data for.
   * @param start
   *          Earliest time in the range of interest.
   * @param end
   *          Most recent time in the range of interest.
   * @return Vector containing all data for the point over the time range.
   */
  public Vector<PointData> extract(PointDescription pm, AbsTime start, AbsTime end) {
    try {
      if (!isConnected()) {
        System.out.println(databaseName+ " is not connected...");
        return null;
      }
      // Build and execute the query, selecting all data for name of point description object
      String cmd = "select * from " + pm.getName().toString()
              + " where time >=" + start.getValue()
              + " and time <=" + end.getValue();
      QueryResult qout;
      synchronized (influxDB) {
        Query query = new Query(cmd, databaseName);
        qout = influxDB.query(query);
        qout.getResults();
      }
      // Ensure we recieved data
      if (qout.getResults().isEmpty()) {
        System.out.println("Query returns null");
        return null;
      }
      // Finished querying
      Vector<PointData> res = new Vector<PointData>(1000, 8000);
      //construct pointData object from query result object
      PointData qres = new PointData("Query", qout);
      //add qres pointdata elements to res pointdata vector
      for(int i =0;res.size()<=i;) {
        res.add(qres);
        i++;
      }
      //return query as pointdata vector
      return res;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Extract data from the archive with no undersampling.
   *
   * @param pm
   *          Point to extract data for.
   * @param start
   *          Earliest time in the range of interest.
   * @param end
   *          Most recent time in the range of interest.
   * @return Vector containing all data for the point over the time range.
   */
  protected Vector<PointData> extractDeep(PointDescription pm, AbsTime start, AbsTime end){
    try {
      if (!isConnected()) {
        System.out.println(databaseName+ " is not connected...");
        return null;
      }
      // Build and execute the query, selecting all data for name of point description object
      String cmd = "select * from " + pm.getName().toString()
                                    + " where time >=" + start.getValue()
                                    + " and time <=" + end.getValue();
      Query query = new Query(cmd, databaseName);
      QueryResult qout = influxDB.query(query);
      qout.getResults();
      // Ensure we recieved data
      if (qout.getResults().isEmpty()) {
        System.out.println("Query returns null");
        return null;
      }
      // Finished querying
      Vector<PointData> res = new Vector<PointData>(1000, 8000);
      //construct pointData object from query result object
      PointData qres = new PointData("Query", qout);
      //add qres pointdata elements to res pointdata vector
      for(int i =0;res.size()<=i;) {
        res.add(qres);
        i++;
      }
      //return query as pointdata vector
      return res;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Method to do the actual archiving.
   *
   * @param pm The point whos data we wish to archive.
   *
   * @param pd Vector of data to be archived.
   */
  protected void saveNow(PointDescription pm, Vector<PointData> pd) {
    Long pointTime;
    Object pointValueObj;
    double pointValue = 0.0;
    boolean pointAlarm;
    String pointUnits = pm.getUnits();
    String pointName = pm.getSource() + "." + pm.getName();

    if(itsShuttingDown) {
      return;
    }

    if (!isConnected()) {
      try {
        setUp();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    final Map<String,String> tagValues = new HashMap<String, String>();
    //final TagExtractor.Holder nameHolder = new TagExtractor.Holder(pointName.replaceAll("trd_lrd", "timing"));
    final TagExtractor.Holder nameHolder = new TagExtractor.Holder(pointName);
    tags.forEach((n, te)-> te.apply(nameHolder).ifPresent(v -> tagValues.put(n, v)));

    // field name is last part of MoniCA point name
    // measurement name is the first part
    String name = nameHolder.get();
    int valueNameSepIndex = name.lastIndexOf(".");
    String measurementName = name.substring(0, valueNameSepIndex);
    String fieldName = name.substring(valueNameSepIndex+1);

      synchronized (pd) {
        for (int i = 0; i < pd.size(); i++) {
              try { //extract point data and metadata to write to influx
                PointData pointData = pd.get(i);

                // pointTime in a BAT time so convert to epoch
                pointTime = pointData.getTimestamp().getAsDate().getTime();
                if (pointTime < itsOldestPointTime || itsOldestPointTime == 0) {
                  itsOldestPointTime = pointTime;
                }
                if (pointTime > itsNewestPointTime || itsNewestPointTime == 0) {
                  itsNewestPointTime = pointTime;
                }
                pointValueObj = pointData.getData();
                if (pointValueObj instanceof Number) {
                    pointValue = ((Number)pointValueObj).doubleValue();
                }
                pointAlarm = pointData.getAlarm();
                //itsLogger.info(/"CRH measurement " + measurementName + " time " + pointTime + " tags " + tagValues);
              } catch (Exception e) {
                e.printStackTrace();
                continue;
              }
              try { //flush to influx
                Point point1 = Point.measurement(measurementName)
                        .time(pointTime, TimeUnit.MILLISECONDS)
                        .addField(fieldName, pointValue)
                        //.addField("Units", pointUnits)
                        //.addField("Alarm", pointAlarm)
                        .tag(tagValues)
                        .build();
                  batchPoints.point(point1);
              } catch (Exception a) {
                a.printStackTrace();
              }
        }
        //itsLogger.info("flushing " + pd.size() + " points from " + itsOldestPointTime + " to " + itsNewestPointTime);
        long elapsedPoint = Instant.now().toEpochMilli() - itsOldestPointTime;
        long elapsedWrite = Instant.now().toEpochMilli() - itsLastWriteTime;
        //if (itsOldestPointTime > 0) {
        //    elapsed = Instant.now().toEpochMilli() - itsOldestPointTime;
        //}
        //else {
        //  itsOldestPointTime = Instant.now().toEpochMilli();
        //}
        if (elapsedWrite > theirInfluxBatchAge || batchPoints.getPoints().size() >= theirInfluxBatchSize) {
          itsLogger.info("sending " + batchPoints.getPoints().size() + " to influxdb age " + elapsedPoint);
          influxDB.write(batchPoints);
          batchPoints.getPoints().clear();
          itsOldestPointTime = 0;
          itsNewestPointTime = 0;
          itsLastWriteTime = Instant.now().toEpochMilli();
        }
        else {
          //itsLogger.info("not archiving " + pd.size() + " points, elapsed " + elapsed);
        }
        synchronized (itsBeingArchived) {
          itsBeingArchived.remove(pm.getFullName());
        }
        pd.clear();
        if ( itsShuttingDown) {
          itsLogger.info("closing influxdb connection");
          influxDB.close();
          influxDB = null;
          pd.clear();
        }
      }
  }

  /**
   * Return the last update which precedes the specified time. We interpret 'precedes' to mean data_time<=req_time.
   *
   * @param pm
   *          Point to extract data for.
   * @param ts
   *          Find data preceding this timestamp.
   * @return PointData for preceding update or null if none found.
   */
  protected PointData getPrecedingDeep(PointDescription pm, AbsTime ts) {
    try {
      if (!isConnected()) {
        System.out.println(databaseName+ " is not connected...");
        return null;
      }
      // Build and execute the query, selecting all data for name of point description object
      String cmd = "select * from " + pm.getName().toString()
                                    + " where time <=" + ts.getValue();
      Query query = new Query(cmd, databaseName);
      QueryResult qout = influxDB.query(query);
      qout.getResults();
      // Ensure we recieved data
      if (qout.getResults().isEmpty()) {
        System.out.println("Query returns null");
        return null;
      }
      //return the extracted data
      return new PointData("Query", qout);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Return the first update which follows the specified time. We interpret 'follows' to mean data_time>=req_time.
   *
   * @param pm Point to extract data for.
   *
   * @param ts Find data following this timestamp.
   *
   * @return PointData for following update or null if none found.
   */
  protected PointData getFollowingDeep(PointDescription pm, AbsTime ts) {
    try {
      if (!isConnected()) {
        System.out.println(databaseName+ " is not connected...");
        return null;
      }
      // Build and execute the query, selecting all data for name of point description object
      String cmd = "select * from " + pm.getName().toString()
                                    + " where time >=" + ts.getValue();
      Query query = new Query(cmd, databaseName);
      QueryResult qout= influxDB.query(query);
      qout.getResults();
      // Ensure we recieved data
      if (qout.getResults().isEmpty()) {
        System.out.println("Query returns null");
        return null;
      }
      //return the extracted data
      return new PointData("Query", qout);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Purge all data for the given point that is older than the specified age in days.
   *
   * @param pd
   *          The point whos data we wish to purge.
   */
  protected void purgeOldData(PointDescription pd){
    if (pd.getArchiveLongevity() <= 0){
      System.out.println("Conflict with point archive policy");
      return;
    }
    // Check server connection
    if (!isConnected()) {
      return;
    }
    try {
      // Build and execute the query, selecting all data for name of point description object
      String cmd = "delete * from " + pd.getName().toString();
      Query query = new Query(cmd, databaseName);
      influxDB.query(query);
    } catch (Exception e) {
      e.printStackTrace();
      }
    }

  /**
   * Flush input point data argument to influx within maximum chunk specification
   *
   * @param pd
   *         Input point to flush
   * @throws IOException
   */
  public void flushPoint(Point pd) throws IOException {
    if (batchPoints == null || (chunkSize > 0 &&  totalSize%chunkSize ==0)) {
      // close current chunk
      flush();
    }
    batchPoints.point(pd);
    totalSize++;
  }

  /**
   * Flush all points in batchPoints to influx
   * @throws IOException
   */
  public void flush() throws IOException {
    if (batchPoints != null && isConnected()) {
      influxDB.write(batchPoints);
    }
  }

  /**
   * Calls flush and closes connection to influxDB database
   * 
   * @throws IOException
   */
  public void close() throws IOException {
    flush();
    influxDB.close();
  }
}
