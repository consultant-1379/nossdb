package com.distocraft.dc5000.etl.nossdb;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.etl.parser.Main;
import com.distocraft.dc5000.etl.parser.MeasurementFile;
import com.distocraft.dc5000.etl.parser.Parser;
import com.distocraft.dc5000.etl.parser.SourceFile;

/**
 * NOSSDB Parser <br>
 * <br>
 * Configuration: <br>
 * <br>
 * Database usage: Not directly <br>
 * <br>
 * Copyright Distocraft 2005 <br>
 * <br>
 * $id$ <br>
 * 
 * @author lemminkainen
 */
public class NOSSDBParser implements Parser {

  public static final String DELIMITER = ";";

  private String techPack;

  private String setType;

  private String setName;

  private int status = 0;

  private Main mainParserObject = null;

  private String workerName = "";
  
  private Logger log;

  final private List errorList = new ArrayList();


  /**
   * Just-hang-around-constructor
   */
  public NOSSDBParser() {}

  /**
   * Parse one SourceFile
   * 
   * @see com.distocraft.dc5000.etl.parser.Parser#parse(com.distocraft.dc5000.etl.parser.SourceFile,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  public void parse(SourceFile sf, String techPack, String setType, String setName) throws Exception {
    log = Logger.getLogger("etl." + techPack + "." + setType + "." + setName + ".parser.NOSSDB");

    BufferedReader br = null;
    MeasurementFile mFile = null;

    try {

      br = new BufferedReader(new InputStreamReader(sf.getFileInputStream()));

      List columns = null;

      String line;
      while ((line = br.readLine()) != null) {

        line = line.trim();

        if (line.length() <= 0) { // Incoming header!

          if (mFile != null) {
            try {
              mFile.close();
            } catch (Exception e) {
              log.log(Level.WARNING, "Error closing MeasurementFile", e);
            }
          }

          columns = readHeader(line);

          String vendorTag = null;
          
          try {

            String mTypeIDs = sf.getProperty("measurementTypeIDs");
            List typeIDList = new ArrayList();
            
            StringTokenizer tokens = new StringTokenizer(mTypeIDs, ",", false);
            while (tokens.hasMoreTokens()) {
              typeIDList.add(tokens.nextToken());
            }

            String orig = sf.getName().substring(0, sf.getName().lastIndexOf("_"));

            
            if (typeIDList==null || typeIDList.isEmpty()){

              vendorTag = orig;
              
            }else{

              vendorTag = Integer.toString(1 + typeIDList.indexOf(orig));

              
            }


          } catch (Exception e) {
            throw new Exception("Mallformed filename " + sf.getName(), e);
          }

          log.finer("VendorTag: " + vendorTag);

          mFile = Main.createMeasurementFile(sf, vendorTag, techPack, setType, setName, workerName, log);
          br.readLine(); // ignore ---;--- row

        } else { // actual datarow

          readData(line, columns, mFile);

          mFile.saveData();

        }

      } // while ((line = br.readLine()) != null)

    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (Exception e) {
          log.log(Level.WARNING, "Error closing Reader", e);
        }
      }

      if (mFile != null) {
        try {
          mFile.close();
        } catch (Exception e) {
          log.log(Level.WARNING, "Error closing MeasurementFile", e);
        }
      }
    }

  }

  /**
   * Reads header lines.
   * 
   * @param line
   *          The reader
   * @return List of discovered columnNames
   * @throws Exception
   *           is thrown in case of failure
   */
  private List readHeader(String line) throws Exception {

    List cols = new ArrayList();

    StringTokenizer tz = new StringTokenizer(line, DELIMITER);

    while (tz.hasMoreTokens()) {
      String col = tz.nextToken();
      cols.add(col);
    }

    return cols;

  }

  /**
   * Reads datalines.
   * 
   * @param line
   *          A line of data
   * @param cols
   *          Column names
   * @param mFile
   *          MeasurementFile
   * @throws Exception
   *           in case of failure
   */
  private void readData(String line, List cols, MeasurementFile mFile) throws Exception {

    StringTokenizer tz = new StringTokenizer(line, DELIMITER);

    int colix = 0;

    while (tz.hasMoreTokens() && colix < cols.size()) {
      String token = tz.nextToken();

      mFile.addData((String) cols.get(colix), token);

      colix++;
    }

  }

  /**
   * 
   */
  public void init(final Main main, final String techPack, final String setType, final String setName,
      final String workerName) {
    this.mainParserObject = main;
    this.techPack = techPack;
    this.setType = setType;
    this.setName = setName;
    this.status = 1;
    this.workerName = workerName;

    String logWorkerName = "";
    if (workerName.length() > 0) {
      logWorkerName = "." + workerName;
    }

    log = Logger.getLogger("etl." + techPack + "." + setType + "." + setName + ".parser.NOSSDBParser" + logWorkerName);
  }

  public int status() {
    return status;
  }
  
  public List errors() {
    return errorList;
  }

  public void run() {

    try {

      this.status = 2;
      SourceFile sf = null;

      while ((sf = mainParserObject.nextSourceFile()) != null) {

        try {

          mainParserObject.preParse(sf);
          parse(sf, techPack, setType, setName);
          mainParserObject.postParse(sf);
        } catch (Exception e) {
          mainParserObject.errorParse(e, sf);
        } finally {
          mainParserObject.finallyParse(sf);
        }
      }
    } catch (Exception e) {
      // Exception catched at top level. No good.
      log.log(Level.WARNING, "Worker parser failed to exception", e);
      errorList.add(e);
    } finally {
      this.status = 3;
    }
  }

}