package atnf.atoms.mon.archiver;

import atnf.atoms.mon.PointData;
import atnf.atoms.mon.PointDescription;
import atnf.atoms.mon.util.MonitorConfig;
import atnf.atoms.time.AbsTime;
import au.csiro.pbdava.diamonica.etl.influx.TagExtractor;
import au.csiro.pbdava.diamonica.etl.influx.OrderedProperties;
import java.io.IOException;
import java.time.Instant;
import java.util.Vector;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;
import org.influxdb.InfluxDB.LogLevel;
import org.apache.log4j.Logger;

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

public class PointArchiverInfluxDb extends PointArchiver {

  /**
   * InfluxDB database name
   */
  private static String databaseName = MonitorConfig.getProperty("InfluxDB", "testing");

  /**
   * Global retention policy
   */
  private static String retentionPolicy = MonitorConfig.getProperty("InfluxRetensionPolicy", "autogen");

  /**
   * The URL to connect to the server/database.
   */
  private static String theirURL = MonitorConfig.getProperty("InfluxURL", "http://localhost:8086");

  /**
   * Influx username
   */
  private static String theirUsername = MonitorConfig.getProperty("InfluxUsername", "admin");

  /**
   * Influx password
   */
  private static String theirPassword = MonitorConfig.getProperty("InfluxPassword", "admin");

  /**
   * Tag file location
   */
  private static String tagFilePath = MonitorConfig.getProperty("InfluxTagMap", "");

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
  private BlockingQueue<BatchPoints> itsBatchQueue = new LinkedBlockingQueue<BatchPoints>();
  private BatchPoints itsCurrentBatch = null;
  private long itsOldestPointTime = 0;
  private long itsBatchStart = 0;
  private BatchPoints itsMetadataBatch = null;

  Map<PointDescription, Map<String, String>> itsTagMap = new HashMap<>();
  Map<PointDescription, String> itsMeasurementNameMap = new HashMap<>();
  Map<PointDescription, String> itsFieldNameMap = new HashMap<>();

  /**
   * The connection to the InfluxDB server.
   */
  private InfluxDB influxDB = null;


  /** Static block to parse flush parameters. */
  static {
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
    this.influxDB = InfluxDBFactory.connect(theirURL, theirUsername, theirPassword);
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
    tagProperties.getProperties().forEach((k, v) -> tags.put(k, TagExtractor.fromString(v)));

    // create databases
    influxDB.createDatabase(databaseName);
    influxDB.deleteDatabase(databaseName + "_metadata");
    influxDB.createDatabase(databaseName + "_metadata");
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
   * @param pm    Point to extract data for.
   * @param start Earliest time in the range of interest.
   * @param end   Most recent time in the range of interest.
   * @return Vector containing all data for the point over the time range.
   */
  public Vector<PointData> extract(PointDescription pm, AbsTime start, AbsTime end) {
    try {
      if (!isConnected()) {
        System.out.println(databaseName + " is not connected...");
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
      for (int i = 0; res.size() <= i; ) {
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
   * @param pm    Point to extract data for.
   * @param start Earliest time in the range of interest.
   * @param end   Most recent time in the range of interest.
   * @return Vector containing all data for the point over the time range.
   */
  protected Vector<PointData> extractDeep(PointDescription pm, AbsTime start, AbsTime end) {
    try {
      if (!isConnected()) {
        System.out.println(databaseName + " is not connected...");
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
      for (int i = 0; res.size() <= i; ) {
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
   * @param pd Vector of data to be archived.
   */
  protected void saveNow(PointDescription pm, Vector<PointData> pd) {
    if (itsShuttingDown) {
      return;
    }
    long startTime = System.nanoTime();

    if (!isConnected()) {
      try {
        setUp();
        runThread.start();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    Map<String, String> tagValues;
    String measurementName;
    String fieldName;

    if (itsTagMap.containsKey(pm)) {
      tagValues = itsTagMap.get(pm);
      measurementName = itsMeasurementNameMap.get(pm);
      fieldName = itsFieldNameMap.get(pm);
    } else {
      String pointName = pm.getSource() + "." + pm.getName();
      Map<String, String> tv = new HashMap<String, String>();
      final TagExtractor.Holder nameHolder = new TagExtractor.Holder(pointName);
      tags.forEach((n, te) -> te.apply(nameHolder).ifPresent(v -> tv.put(n, v)));

      // add unit as a tag
      String pointUnits = pm.getUnits();
      if (pointUnits != null && pointUnits.length() > 0) {
        tv.put("units", pointUnits);
      }

      // field name is last part of MoniCA point name
      // measurement name is the first part
      String name = nameHolder.get();
      int valueNameSepIndex = name.lastIndexOf(".");
      measurementName = name.substring(0, valueNameSepIndex);
      fieldName = name.substring(valueNameSepIndex + 1);
      itsTagMap.put(pm, tv);
      itsMeasurementNameMap.put(pm, measurementName);
      itsFieldNameMap.put(pm, fieldName);
      tagValues = tv;
      try {
        if (itsMetadataBatch == null) {
          itsMetadataBatch = BatchPoints
                  .database(databaseName + "_metadata")
                  .retentionPolicy(retentionPolicy)
                  .consistency(InfluxDB.ConsistencyLevel.ALL)
                  .build();
        }
        HashMap<String, String> metadataTags = new HashMap<>(tagValues);
        metadataTags.remove("antenna");
        String fieldValue = pm.getLongDesc() + "," + pm.getUnits() + "," + pm.getInputTransactionString();
        Point point1 = Point.measurement(measurementName)
                .time(0, TimeUnit.MILLISECONDS)
                .addField(fieldName, fieldValue)
                .tag(metadataTags)
                .build();
        itsMetadataBatch.point(point1);
        if (itsMetadataBatch.getPoints().size() >= theirInfluxBatchSize) {
          itsBatchQueue.add(itsMetadataBatch);
          itsMetadataBatch = null;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    if (itsCurrentBatch == null) {
      itsCurrentBatch = BatchPoints
              .database(databaseName)
              .retentionPolicy(retentionPolicy)
              .consistency(InfluxDB.ConsistencyLevel.ALL)
              .build();
      itsBatchStart = Instant.now().toEpochMilli();
    }

    Long pointTime;
    double pointValue = 0.0;
    for (int i = 0; i < pd.size(); i++) {
      try { //extract point data and metadata to write to influx
        PointData pointData = pd.get(i);
        boolean pointAlarm;

        // pointTime in a BAT time so convert to epoch
        pointTime = pointData.getTimestamp().getAsDate().getTime();
        if (pointTime < itsOldestPointTime || itsOldestPointTime == 0) {
          itsOldestPointTime = pointTime;
        }
        pointAlarm = pointData.getAlarm();

        Point.Builder pb = Point.measurement(measurementName)
                .time(pointTime, TimeUnit.MILLISECONDS)
                .tag(tagValues);

        Object pointValueObj = pointData.getData();
        if (pointData.getData() instanceof Double) {
          pb.addField(fieldName, ((Number)pointValueObj).doubleValue());
        }
        else if (pointData.getData() instanceof Float) {
          pb.addField(fieldName, ((Number)pointValueObj).floatValue());
        }
        else if (pointData.getData() instanceof Integer) {
          pb.addField(fieldName, ((Number)pointValueObj).intValue());
        }
        else if (pointValueObj instanceof String) {
          pb.addField(fieldName, (String)pointValueObj);
        }
        itsCurrentBatch.point(pb.build());
      } catch (Exception a) {
        a.printStackTrace();
      }
    }

    long elapsedPoint = Instant.now().toEpochMilli() - itsOldestPointTime;
    long batchAge = Instant.now().toEpochMilli() - itsBatchStart;
    if (itsCurrentBatch.getPoints().size() >= theirInfluxBatchSize || batchAge > theirInfluxBatchAge) {
      try {
        if (itsMetadataBatch != null) {
          itsBatchQueue.add(itsMetadataBatch);
          itsMetadataBatch = null;
        }
        itsLogger.info("adding batch to queue " + itsCurrentBatch.getPoints().size() + " points age " + elapsedPoint + "ms");
        itsBatchQueue.put(itsCurrentBatch);
        itsCurrentBatch = null;
        itsOldestPointTime = 0;
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    pd.clear();
    synchronized (itsBeingArchived) {
      itsBeingArchived.remove(pm.getFullName());
    }
  }

  Thread runThread = new Thread(new Runnable() {
    @Override
    public void run() {
      long rateStart = Instant.now().toEpochMilli();
      long pointsPerMinute = 0;
      long rateElapsed = 0;
      while (!itsShuttingDown) {
        try {
          BatchPoints batchPoints = itsBatchQueue.take();
          long startTime = System.nanoTime();
          influxDB.write(batchPoints);
          long elapsed = (System.nanoTime() - startTime) / 1000000;
          long elapsedPoint = Instant.now().toEpochMilli() - itsOldestPointTime;
          itsLogger.info("sent " + batchPoints.getPoints().size()
                  + " to influxdb in " + elapsed + "ms depth " + itsBatchQueue.size());
          rateElapsed = Instant.now().toEpochMilli() - rateStart;
          pointsPerMinute += batchPoints.getPoints().size();
          if (rateElapsed >= 60000) {
            double rate = 1.0 * pointsPerMinute / 60;
            itsLogger.info(String.format("ingest rate %.1f points per second", rate));
            rateStart = Instant.now().toEpochMilli();
            pointsPerMinute = 0;
          }
        } catch (Exception e) {
          e.printStackTrace();
        }

      }
      itsLogger.info("closing influxdb connection");
      influxDB.close();
      influxDB = null;
    }
  });

  /**
   * Return the last update which precedes the specified time. We interpret 'precedes' to mean data_time<=req_time.
   *
   * @param pm Point to extract data for.
   * @param ts Find data preceding this timestamp.
   * @return PointData for preceding update or null if none found.
   */
  protected PointData getPrecedingDeep(PointDescription pm, AbsTime ts) {
    try {
      if (!isConnected()) {
        System.out.println(databaseName + " is not connected...");
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
   * @param ts Find data following this timestamp.
   * @return PointData for following update or null if none found.
   */
  protected PointData getFollowingDeep(PointDescription pm, AbsTime ts) {
    try {
      if (!isConnected()) {
        System.out.println(databaseName + " is not connected...");
        return null;
      }
      // Build and execute the query, selecting all data for name of point description object
      String cmd = "select * from " + pm.getName().toString()
              + " where time >=" + ts.getValue();
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
   * Purge all data for the given point that is older than the specified age in days.
   *
   * @param pd The point whos data we wish to purge.
   */
  protected void purgeOldData(PointDescription pd) {
    if (pd.getArchiveLongevity() <= 0) {
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
}


