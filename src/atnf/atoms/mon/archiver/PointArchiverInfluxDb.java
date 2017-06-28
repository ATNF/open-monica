package atnf.atoms.mon.archiver;

import atnf.atoms.mon.PointData;
import atnf.atoms.mon.PointDescription;
import atnf.atoms.time.AbsTime;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;
import org.influxdb.InfluxDB.LogLevel;


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

  private InfluxDB influxDB;

  /**
   *  InfluxDB port number (default)
   */
  private final static int port = 8089;

  /**
   * InfluxDB Diamonica database name
   */
  private static String databaseName = "askap_realtime";

  /**
   * Global retention policy
   */
  private static String retentionPolicy = "autogen";

  /**
   * The connection to the InfluxDB server.
   */
  private Connection itsConnection = null;

  /**
   * The URL to connect to the server/database.
   */
  private String itsURL = "http://localhost:8086";

  /**
   * Tag file location
   */
  private String tagFilePath = "/askap/default/config/askap.properties";

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

  /**
   * Tag Extraction private class
   */
  private class TagExtractor{

    private String tagfile;
    private String replace;
    private Pattern pattern;
    private Map<String, String> tags;
    private static final String REGEX_DOT_OR_END = "($|[._])";
    private static final String REGEX_DOT_OR_START = "(^|[._])";

    public TagExtractor(String pattern) {
      this(pattern, "");
    }

    public TagExtractor(String pattern, String replace) {
      this.pattern = Pattern.compile(REGEX_DOT_OR_START + "(" + pattern + ")" + REGEX_DOT_OR_END);
      this.replace = replace;
    }

    public TagExtractor(Map<String, String> tags){
      this.tags = tags;
    }

    /**
     *
     * @param s input string to convert to tag convention
     * @return TagExtractor object as Map<String, String> of tags
     */
    public TagExtractor fromString(String s) {
      String[] parts = s.split(" ");
      return (parts.length) > 1 ? new TagExtractor(parts[0], parts[1]):new TagExtractor(parts[0]);
    }
    public TagExtractor(){}

    /**
     *
     * @param pathToTagFile, file path to "site".properties file with regex strings
     * @return tagExtactor object (Map<String, String>)
     * @throws IOException
     */
    public TagExtractor fromProperties(String pathToTagFile) throws IOException {
      final Properties tagProperties = new Properties();
      File tagFile = new File(pathToTagFile);
      FileInputStream fileInput = new FileInputStream(tagFile);
      tagProperties.load(fileInput);
      //LoanUtils.withCloseable(new FileInputStream(pathToTagFile), tagProperties::load); ORIGINAL implementation
      final Map<String, String> tagMap = new HashMap<String, String>();
      tagProperties.forEach((k,v) -> tagMap.put(k.toString(), v.toString()));
      return new TagExtractor(tagMap);
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
  }

  /**
   * Check if we are connected to the server and reconnect if required.
   *
   * @return True if connected (or reconnected). False if not connected.
   */
  protected boolean isConnected() {
    boolean influxDBConnection = false;
    this.influxDB = InfluxDBFactory.connect((itsURL + ":" + port), "admin", "admin");
    Pong response;
    try {
      response = this.influxDB.ping();
      if (!response.getVersion().equalsIgnoreCase("unknown")) {
        influxDBConnection = true;
      }
    } catch (Exception e) {
      System.out.println("InfluxDB not connected: ");
      e.printStackTrace();
    }
    return influxDBConnection;
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
      synchronized (itsConnection) {
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
    boolean pointAlarm;
    String pointUnits = pm.getUnits();
    Map<String, String> tagMap;
    TagExtractor tagExtractor = new TagExtractor();
    try {
      setUp();
    } catch (Exception e) {
      e.printStackTrace();
    }
    while (isConnected()) {
      synchronized (pd) {
        for (int i = 0; i < pd.size(); i++) {
              try { //extract point data and metadata to write to influx
                PointData pointData = pd.get(i);
                pointTime = pointData.getTimestamp().getValue();
                pointValueObj = pointData.getData();
                pointAlarm = pointData.getAlarm();
                //get tags
                tagMap = tagExtractor.fromProperties(tagFilePath).tags;
              } catch (Exception e) {
                e.printStackTrace();
                continue;
              }
              try { //flush to influx
                Point point1 = Point.measurement(pm.getName())
                        .time(pointTime, TimeUnit.SECONDS)
                        .addField("Units", pointUnits)
                        .addField(pm.getName(), pointValueObj.toString())
                        .addField("Alarm", pointAlarm)
                        .tag(tagMap)
                        .build();
                batchPoints.point(point1);
                influxDB.write(batchPoints);
              } catch (Exception a) {
                a.printStackTrace();
              }
        } //close database and clear point data objects
        pd.clear();
        influxDB.close();
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
    if (batchPoints != null) {
      boolean connection = isConnected();
      if (!connection) {
        try {
          setUp();
        }catch (Exception e){
          e.printStackTrace();
        }
      }
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
