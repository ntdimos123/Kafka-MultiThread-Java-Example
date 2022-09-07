package com.howtoprogram.kafka.auxiliary;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class Utilities {

  static final String digits = "0123456789";
  /* Testing volatile variable */
  static volatile int sharedVar_GPS = 0;
  static volatile int sharedVar_HEADING = 0;
  static SecureRandom rnd = new SecureRandom();

  KafkaProducer producer = new KafkaProducer();

  Logger logger = LoggerFactory.getLogger(Utilities.class);
  Marker interesting = MarkerFactory.getMarker("CUSTOM");

  public static double[] pointAt(double x0, double y0, double bearing, double distance) {
        /*
         φ2 = asin( sin φ1 ⋅ cos δ + cos φ1 ⋅ sin δ ⋅ cos θ )
         λ2 = λ1 + atan2( sin θ ⋅ sin δ ⋅ cos φ1, cos δ − sin φ1 ⋅ sin φ2 )
         where
         φ is latitude,
         λ is longitude,
         θ is the bearing (clockwise from north),
         δ is the angular distance d/R;
         d being the distance travelled, R the earth’s radius
         */
    double EARTH_RADIUS = 6371.01 * 1000;
    double[] final_coords = new double[2];

    double phi1 = Math.toRadians(x0);
    double l1 = Math.toRadians(y0);
    double delta = distance / EARTH_RADIUS; // normalize linear distance to radian angle

    double phi2 = Math.toDegrees((Math.asin(
        Math.sin(phi1) * Math.cos(delta) + Math.cos(phi1) * Math.sin(delta) * Math.cos(bearing))));
    double l2 = l1 + Math.atan2(Math.sin(bearing) * Math.sin(delta) * Math.cos(phi1),
        Math.cos(delta) - Math.sin(phi1) * Math.sin(phi2));

    double l2_harmonised = Math.toDegrees(
        ((l2 + 3 * Math.PI) % (2 * Math.PI) - Math.PI)); // normalise to −180..+180°

    final_coords[0] = phi2;
    final_coords[1] = l2_harmonised;

    return final_coords;
  }

  public void parseJSON(String message) throws FileNotFoundException {
    JSONParser parser = new JSONParser();
    JSONObject object = new JSONObject();

    try {
      // Try to parse message
      object = (JSONObject) parser.parse(message);
    } catch (org.json.simple.parser.ParseException e) {
      logger.error("(ERROR) Unable to parse message!");
    }

    /**
     * Get the body and transform it before sending it to the topic.
     */
    JSONObject body = (JSONObject) object.get("body");
    JSONObject updatedBody = null;

    /**
     * Get the header and update it before sending it to the topic.
     */
    JSONObject header = (JSONObject) object.get("header");

    // Keep sender's name for later usage
    String sender = header.get("sender").toString().toLowerCase();

    logger.info(interesting, "Received message from: " + sender);
    logger.info(interesting, message);

    PrintWriter p = new PrintWriter(
        new FileOutputStream("LogFileBroker.txt", true)
    );

    p.print(message + "\n");
    p.close();

    String uuid = UUID.randomUUID().toString();

    JSONObject updatedHeader = updateHeader(header, body, uuid);

    if (body.containsKey("reference") && body.containsKey("data")) {
      if (sender.contentEquals("certh_multi")) {
        updatedBody = updateSimilarImages(updatedHeader, body, sender, uuid);
      }
    }

    if (body.containsKey("sequence")) {
      if (!sender.contentEquals("certh_multi")) {
        updatedBody = updateSequence(updatedHeader, body, sender, uuid);
      }
    }

    if (body.containsKey("detectedActivities")) {
      if (!sender.contentEquals("certh_multi")) {
        updatedBody = updateActivities(updatedHeader, body, sender, uuid);
      }
    }

    if (body.containsKey("Parameters")) {
      updatedBody = updateParameters(updatedHeader, body, sender, uuid);
    }

    if (body.containsKey("tweets")) {
      updatedBody = updateTweets(updatedHeader, body, sender, uuid);
    }

    if (body.containsKey("attachments") && !sender.contains("kiosk")) {
      updatedBody = updateAttachments(updatedHeader, body, sender, uuid);
    }

    if (body.containsKey("zones")) {
      updatedBody = updateZones(updatedHeader, body, sender, uuid);
    }

    if (body.containsKey("Attachments") && sender.contains("kiosk")) {
      updatedBody = updateAttachmentsIDM(updatedHeader, body, sender, uuid);
    }

    if (updatedBody == null) {
      if (body.containsKey("detectedActivities")) {
        if (sender.contentEquals("certh_multi")) {
          body.remove("source");
          body.put("dataKey", "CERTH_ACT");
        } else {
          body.put("dataKey", sender.toUpperCase());
          body.remove("source");
        }
      } else if (body.containsKey("relevantGlobalIDs")) {
        body.put("dataKey", body.get("source"));
        body.remove("source");
      } else if (body.containsKey("sequence")) {
        if (sender.contentEquals("certh_multi")) {
          body.remove("source");
          body.put("dataKey", "CERTH_OBJ");
        } else {
          body.put("dataKey", sender.toUpperCase());
          body.remove("source");
        }
      } else {
        body.put("dataKey", sender.toUpperCase());
        body.remove("source");
      }
      sendMessage(updatedHeader, body, uuid, sender);
    }
  }

  public JSONObject updateHeader(JSONObject header, JSONObject body, String uuid) {
    String topic = header.get("topicName").toString().toLowerCase();

    header.remove("topicName");
    if ("topic_15".equals(topic)) {
      header.put("topicName", "TOPIC_15");
    } else {
      header.put("topicName", "TOPIC_04");
    }

    header.remove("msgIdentifier");
    header.put("msgIdentifier", uuid);

    header.remove("sentUTC");
    header.put("sentUTC", Instant.now().toString());

    // The following keeps only seconds. Keep it here for reference.
    String sender = header.get("sender").toString().toLowerCase();
    header.remove("recipients");

    // SENDER PRISMA
    if (sender.contains("prisma_gps")) {
      /* Incrementing shared variable by 1 */
      sharedVar_GPS += 1;
      header.put("recipients", "NTUA_CRISIS,CENTRIC_DECISION,SIMAVI_UI,ADS_REPORT");
    } else if (sender.contains("prisma_ast") || sender.contains("prisma_ra") || sender.contains(
        "prisma_heading")) {
      if (sender.contains("prisma_heading")) {
        sharedVar_HEADING += 1;
      }
      header.put("recipients", "NTUA_CRISIS,CENTRIC_DECISION,ADS_REPORT");
      // SENDER CERTH MMI
    } else if (sender.contains("certh_multi")) {
      if ("topic_15".equals(topic)) {
        header.put("recipients", "CENTRIC_DECISION");
      } else {
        String source = body.get("source").toString().toLowerCase();
        if (source.contains("uuv")) {
          header.put("recipients", "SIMAVI_UI,ADS_REPORT");
        } else {
          header.put("recipients", "NTUA_CRISIS,CENTRIC_DECISION,ADS_REPORT");
        }
      }
    } else if (sender.contains("pct")
        || sender.contains("kiosk")
        || sender.contains("t4i")) {
      header.put("recipients", "NTUA_CRISIS,CENTRIC_DECISION,ADS_REPORT");
    }

    header.remove("sender");
    header.put("sender", "CERTH_ONTOL");

    return header;
  }

  /**
   * This function parses the message that contains the list of similar images and updates their
   * critical level.
   *
   * @param header
   * @param body
   * @param sender
   * @param uuid
   */
  public JSONObject updateSimilarImages(JSONObject header, JSONObject body, String sender,
      String uuid) {
    // get reference pointing to detected image URL
    String reference = body.get("reference").toString();

    // extract image name
    String imageName;
    if (reference.contains("/")) {
      imageName = reference.substring(reference.lastIndexOf('/') + 1);
    } else {
      imageName = reference.substring(reference.lastIndexOf('\\') + 1);
    }

    // get data array
    JSONObject data = (JSONObject) body.get("data");

    // get the URLs of all similar images and the default critical level (0)
    JSONArray attachmentURL = (JSONArray) data.get("attachmentURL");
    // JSONArray criticalLevel = (JSONArray) data.get("criticalLevel");

    // create a hashmap to store the pairs url,level
    Map<String, Integer> hm = new LinkedHashMap<>();

    // start GraphDB related activities
    IRI imageGraph = Repository.connection
        .getValueFactory()
        .createIRI(ISOLA.NAMESPACE, "Images");

    // initialize RDF builder
    ModelBuilder builder = new ModelBuilder()
        .setNamespace("isola", ISOLA.NAMESPACE);

    // create IRI for reference image
    IRI imageObject = Repository.connection
        .getValueFactory()
        .createIRI(ISOLA.NAMESPACE, imageName);

    // initialize the hashmap
    Iterator<String> it = attachmentURL.iterator();
    while (it.hasNext()) {
      String sim = it.next();
      // extract image nameString
      if (sim.contains("/")) {
        imageName = sim.substring(sim.lastIndexOf('/') + 1);
      } else {
        imageName = sim.substring(sim.lastIndexOf('\\') + 1);
      }

      // create IRI for reference image
      IRI simImageObject = Repository.connection
          .getValueFactory()
          .createIRI(ISOLA.NAMESPACE, imageName);

      // link similar images to reference image
      builder
          .namedGraph(imageGraph)
          .subject(imageObject)
          .add(RDF.TYPE, ISOLA.IMAGE)
          .add(ISOLA.HASURL, reference)
          .add(ISOLA.HASSIMILARIMAGE, simImageObject)
          .subject(simImageObject)
          .add(RDF.TYPE, ISOLA.IMAGE)
          .add(ISOLA.HASURL, sim);

      int level = checkCriticalLevel(sim);

      if (level == -1) {
        hm.put(sim, 0);
      } else {
        hm.put(sim, level);
      }
    }

    /* We're done building, create our Model */
    Model model = builder.build();

    /* Commit model to repository */
    Repository.commitModel(model);

    // create new array to keep updated critical levels
    JSONArray updateLevels = new JSONArray();

    Set<String> keys = hm.keySet();

    // printing the elements of LinkedHashMap
    for (String key : keys) {
      updateLevels.add(hm.get(key));
    }

    // remove old levels
    data.remove("criticalLevel");

    // update data
    data.put("criticalLevel", updateLevels);

    // send message to kafka
    sendMessage(header, body, uuid, sender);

    return body;
  }

  public Integer checkCriticalLevel(String URL) {
    String escapedURL;

    escapedURL = URL.replace("\\", "\\\\");

    String queryString;

    queryString = "PREFIX isola: <https://www.semanticweb.org/mklab/isola#> \n";
    queryString += "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n";
    queryString += "select ?url ?level where { \n";
    queryString += "    GRAPH isola:Images { \n";
    queryString += "		?s rdf:type isola:Image . \n";
    queryString += "        ?s isola:hasURL ?url . \n";
    queryString += "        ?s isola:criticalLevel ?level . \n";
    queryString += "    } \n";
    queryString += "    FILTER (?url = \"" + escapedURL + "\") \n";
    queryString += "} \n";

    TupleQuery query = Repository.connection.prepareTupleQuery(queryString);
    Integer level = -1;
    try (TupleQueryResult result = query.evaluate()) {
      if (result.hasNext()) {
        BindingSet solution = result.next();
        level = Integer.parseInt(solution.getBinding("level").getValue().stringValue());
      }
    }

    return level;
  }

  public JSONObject updateZones(JSONObject header, JSONObject body, String sender, String uuid) {
    JSONArray zones = (JSONArray) body.get("zones");

    JSONObject tempBody = new JSONObject();

    tempBody.put("eventID", body.get("eventID"));
    tempBody.put("timeUTC", body.get("timeUTC"));
    tempBody.put("dataKey", sender.toUpperCase());

    for (Object z : zones) {
      JSONObject zone = (JSONObject) z;

      tempBody.put("zoneID", zone.get("zoneID"));
      tempBody.put("zoneName", zone.get("zoneName"));

      JSONObject zoneCoords = (JSONObject) zone.get("zoneCoordinates");

      tempBody.put("x", zoneCoords.get("x"));
      tempBody.put("y", zoneCoords.get("y"));
      tempBody.put("z", zoneCoords.get("z"));

      if (sender.contains("pct")) {
        tempBody.put("count", zone.get("count"));
        tempBody.put("detectedCruiseIDs", zone.get("detectedCruiseIDs"));

        sendMessage(header, tempBody, uuid, sender);
      } else {
        JSONArray concentrations = (JSONArray) zone.get("concentrations");

        for (Object c : concentrations) {
          JSONObject conc = (JSONObject) c;

          tempBody.put("agent", conc.get("agent"));
          tempBody.put("concentration", conc.get("concentration"));
          tempBody.put("currentClassification", conc.get("currentClassification"));
          tempBody.put("nextClassification", conc.get("nextClassification"));
          tempBody.put("nextClassificationTimestamp", conc.get("nextClassificationTimestamp"));
          tempBody.put("isHotspot", conc.get("isHotspot"));

          sendMessage(header, tempBody, uuid, sender);
        }
      }
    }

    return tempBody;
  }

  public JSONObject updateAttachmentsIDM(JSONObject header, JSONObject body, String sender,
      String uuid) {
    JSONArray attachments = (JSONArray) body.get("Attachments");
    JSONObject tempBody = new JSONObject();

    tempBody.put("eventID", body.get("eventID"));
    tempBody.put("TimeUTC", body.get("TimeUTC"));
    tempBody.put("Title", body.get("Title"));
    tempBody.put("Description", body.get("Description"));
    tempBody.put("Latitude", body.get("Latitude"));
    tempBody.put("Longitude", body.get("Longitude"));
    tempBody.put("dataKey", sender.toUpperCase());

    /* Iterate parameters array */
    for (Object a : attachments) {
      JSONObject attachment = (JSONObject) a;

      tempBody.put("attachmentName", attachment.get("attachmentName"));
      tempBody.put("attachmentType", attachment.get("attachmentType"));
      tempBody.put("attachmentTimeUTC", attachment.get("attachmentTimeUTC"));
      tempBody.put("attachmentURL", attachment.get("attachmentURL"));

      sendMessage(header, tempBody, uuid, sender);
    }

    return tempBody;
  }

  public JSONObject updateAttachments(JSONObject header, JSONObject body, String sender,
      String uuid) {
    JSONArray attachments = (JSONArray) body.get("attachments");

    JSONObject tempBody = new JSONObject();

    tempBody.put("source", body.get("source"));
    tempBody.put("planID", body.get("planID"));
    tempBody.put("endTime", body.get("endTime"));
    tempBody.put("startTime", body.get("startTime"));
    tempBody.put("msgType", body.get("msgType"));
    tempBody.put("sent", body.get("sent"));

    if (body.get("source").toString().contentEquals("OMST_UUV")) {
      tempBody.put("globalID", body.get("globalID"));
      tempBody.put("dataKey", "OMST_UUV");
    } else {
      tempBody.put("dataKey", sender.toUpperCase());
    }
    /* Iterate parameters array */
    for (Object a : attachments) {
      JSONObject attachment = (JSONObject) a;

      tempBody.put("sensor", attachment.get("sensor"));
      tempBody.put("Content-Type", attachment.get("contentType"));
      tempBody.put("attachmentURL", attachment.get("attachmentURL"));

      sendMessage(header, tempBody, uuid, sender);
    }

    return tempBody;
  }

  public JSONObject updateTweets(JSONObject header, JSONObject body, String sender, String uuid) {
    JSONArray tweets = (JSONArray) body.get("tweets");

    JSONObject tempBody = new JSONObject();

    tempBody.put("source", body.get("source"));
    tempBody.put("userID", body.get("userID"));
    tempBody.put("username", body.get("username"));
    tempBody.put("userScreenName", body.get("userScreenName"));
    tempBody.put("profileImageURL", body.get("profileImageURL"));
    tempBody.put("profileCreatedAt", body.get("profileCreatedAt"));
    tempBody.put("msgType", body.get("msgType"));
    tempBody.put("sent", body.get("sent"));
    tempBody.put("dataKey", sender.toUpperCase());

    /* Iterate parameters array */
    for (Object t : tweets) {
      JSONObject tweet = (JSONObject) t;

      tempBody.put("tweetID", tweet.get("tweetID"));
      tempBody.put("tweetText", tweet.get("tweetText"));
      tempBody.put("tweetCreatedAt", tweet.get("tweetCreatedAt"));
      tempBody.put("tweetHashTags", tweet.get("tweetHashTags"));

      sendMessage(header, tempBody, uuid, sender);
    }

    return tempBody;
  }

  public JSONObject updateParameters(JSONObject header, JSONObject body, String sender,
      String uuid) {
    JSONArray parameters = (JSONArray) body.get("Parameters");

    body.put("dataKey", sender.toUpperCase());

    /* Iterate parameters array */
    for (Object p : parameters) {
      JSONObject par = (JSONObject) p;

      body.put(par.get("Name"), par.get("value"));
    }

    body.remove("Parameters");

    if (sender.toUpperCase().contains("GPS")) {
      logger.debug("Storing coordinates...");
      parseNMEACoordinates(body, uuid);
    }

    if (sender.toUpperCase().contains("RATTM")) {
      logger.debug("Storing radar info...");
      parseRATTMInfo(body, uuid);
    }

    if (sender.toUpperCase().contains("AST1")) {
      logger.debug("Storing ais info...");
      parseAST1Info(body, uuid);
    }

    sendMessage(header, body, uuid, sender);

    return body;
  }

  public JSONObject updateSequence(JSONObject header, JSONObject body, String sender, String uuid) {
    JSONArray sequence = (JSONArray) body.get("sequence");

    JSONObject tempBody = new JSONObject();

    tempBody.put("source", body.get("source"));
    tempBody.put("attachmentURL", body.get("attachmentURL"));
    tempBody.put("missionID", body.get("missionID"));
    tempBody.put("componentID", body.get("componentID"));
    tempBody.put("height", body.get("height"));
    tempBody.put("width", body.get("width"));
    tempBody.put("fps", body.get("fps"));
    tempBody.put("latitude", body.get("latitude"));
    tempBody.put("longitude", body.get("longitude"));
    tempBody.put("altitude", body.get("altitude"));
    tempBody.put("zoneID", body.get("zoneID"));
    tempBody.put("zoneName", body.get("zoneName"));
    tempBody.put("dataKey", sender.toUpperCase());

    /* Iterate sequence array */
    for (Object s : sequence) {
      JSONObject seq = (JSONObject) s;

      tempBody.put("frameID", body.get("frameID"));

      if (seq.containsKey("detection")) {

        JSONArray detection = (JSONArray) seq.get("detection");

        /* Iterate detection array */
        for (Object d : detection) {
          JSONObject det = (JSONObject) d;

          JSONObject bbox = (JSONObject) det.get("bbox");

          tempBody.put("objectID", det.get("objectID"));
          tempBody.put("class", det.get("class"));
          tempBody.put("confidence", det.get("confidence"));
          tempBody.put("x_min", bbox.get("x_min"));
          tempBody.put("y_min", bbox.get("y_min"));
          tempBody.put("x_max", bbox.get("x_max"));
          tempBody.put("y_max", bbox.get("y_max"));

          sendMessage(header, tempBody, uuid, sender);
        }
      }
    }

    return tempBody;
  }

  public JSONObject updateActivities(JSONObject header, JSONObject body, String sender,
      String uuid) {
    JSONObject detActivities = (JSONObject) body.get("detectedActivities");

    body.remove("detectedActivities");

    body.put("activityID", detActivities.get("activityID"));
    body.put("startTime", detActivities.get("startTime"));
    body.put("endTime", detActivities.get("endTime"));
    body.put("activityName", detActivities.get("activityName"));
    body.put("confidence", detActivities.get("confidence"));
    body.put("dataKey", sender.toUpperCase());

    sendMessage(header, body, uuid, sender);

    return body;
  }

  public void sendMessage(JSONObject updatedHeader, JSONObject updatedBody, String uuid,
      String sender) {
    /**
     *  Create the updated object.
     */
    JSONObject updatedObject = new JSONObject();

    updatedObject.put("header", updatedHeader);
    updatedObject.put("body", updatedBody);

    if (sender.toLowerCase().contains("gps")) {
      if (sharedVar_GPS % 5 == 0) {
        producer.producer(updatedObject.toString());
      }
    } else if (sender.toLowerCase().contains("heading")) {
      if (sharedVar_HEADING % 5 == 0) {
        producer.producer(updatedObject.toString());
      }
    } else {
      producer.producer(updatedObject.toString());
    }
  }

  /* Generates a random string of digits of given length */
  public String randomString(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append(digits.charAt(rnd.nextInt(digits.length())));
    }
    return sb.toString();
  }

  public JSONObject parseJSONLD(String input) {
    // Read JSON file containing information about DMS Input
    JSONParser parser = new JSONParser();

    // Create JSONObject of DMS Input
    JSONObject object = new JSONObject();

    try {
      // Try to parse message
      logger.info("(INFO) Parsing json(-ld) to RDF.");
      object = (JSONObject) parser.parse(input);
    } catch (org.json.simple.parser.ParseException e) {
      logger.error("(ERROR) Unable to parse message!");
    }

    return object;
  }

  public void parseNMEACoordinates(JSONObject body, String uuid) {
    logger.debug("Entering GPS function...");
    GpsJsonldToRdf(body, uuid);
  }

  public void parseRATTMInfo(JSONObject body, String uuid) {
    logger.debug("Entering RATTM function");
    RattmJsonldToRdf(body, uuid);
  }

  public void parseAST1Info(JSONObject body, String uuid) {
    logger.debug("Entering AST1 function...");
    Ast1ToRdf(body, uuid);
  }

  public void GpsJsonldToRdf(JSONObject object, String uuid) {
    logger.debug("In function...");

    Double x_lat = Double.parseDouble(object.get("Latitude").toString());
    Double y_long = Double.parseDouble(object.get("Longitude").toString());

    Integer x_dd = (int) (x_lat / 100.00);
    Double x_mm = x_lat - (x_dd * 100.00);
    Double latDec = x_dd + (x_mm / 60.00);

    Integer y_dd = (int) (y_long / 100.00);
    Double y_mm = y_long - (y_dd * 100.00);
    Double lonDec = y_dd + (y_mm / 60.00);

    long unixTime = System.currentTimeMillis() / 1000L;

    logger.debug("Creating query...");

    String queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> \n";
    queryString += "PREFIX isola: <https://www.semanticweb.org/mklab/isola#> \n";
    queryString += "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n";
    queryString += "PREFIX sosa: <http://www.w3.org/ns/sosa/> \n";
    queryString += "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n";
    queryString += "DELETE { \n";
    queryString += "  ?s geo:hasGeometry ?geo . \n";
    queryString += "  ?geo geo:asWKT ?point .  \n";
    queryString += "  ?s isola:lat ?lat ; \n";
    queryString += "     isola:long ?long ; \n";
    queryString += "     sosa:resultTime ?time \n";
    queryString += "} \n";
    queryString += "WHERE  { \n";
    queryString += "   OPTIONAL { \n";
    queryString += "      ?s geo:hasGeometry ?geo . \n";
    queryString += "      ?geo geo:asWKT ?point . \n";
    queryString += "           ?s isola:lat ?lat ;  \n";
    queryString += "           isola:long ?long ;  \n";
    queryString += "           sosa:resultTime ?time .  \n";
    queryString += "   } \n";
    queryString += "}; \n";
    queryString += "INSERT { \n";
    queryString += "    GRAPH <https://www.semanticweb.org/mklab/isola#GPS/Location> { \n";
    queryString += "          isola:OwnShip geo:hasGeometry [ geo:asWKT ?point] ; \n";
    queryString += "              isola:lat " + latDec + " ; \n";
    queryString += "              isola:long " + lonDec + " ; \n";
    queryString += "			        sosa:resultTime " + unixTime + " . \n";
    queryString += "    } \n";
    queryString += "} WHERE { \n";
    queryString += "	 BIND(CONCAT(\"POINT(\", STR(" + latDec + "), \" \", STR(" + lonDec
        + "), \")\") AS ?point) \n";
    queryString += "} \n";

    logger.debug("Printing query...");
    logger.debug(queryString);

    logger.debug("Executing query...");
    Repository.executeQuery(queryString);
  }

  public void RattmJsonldToRdf(JSONObject object, String uuid) {
    logger.debug("In function...");

    Double distance = Double.parseDouble(object.get("Target-Distance").toString());
    Double bearing = Double.parseDouble(object.get("Bearing_from_own_ship").toString());
    String number = object.get("Target-number").toString();

    long unixTime = System.currentTimeMillis() / 1000L;

    logger.debug("Retrieving ship's location...");

    String queryString = "PREFIX isola: <https://www.semanticweb.org/mklab/isola#> \n";
    queryString += "SELECT ?lat ?lng { \n";
    queryString += "    GRAPH <https://www.semanticweb.org/mklab/isola#GPS/Location> { \n";
    queryString += "        ?s isola:lat ?lat . \n";
    queryString += "        ?s isola:long ?lng . \n";
    queryString += "    } \n";
    queryString += "} \n";

    TupleQuery query = Repository.connection.prepareTupleQuery(queryString);

    Double s_lat = -100000.00;
    Double s_lng = -100000.00;
    try (TupleQueryResult result = query.evaluate()) {
      // we just iterate over all solutions in the result...
      if (result.hasNext()) {
        BindingSet solution = result.next();

        /* Get the IRI of the class */
        s_lat = Double.parseDouble(solution.getBinding("lat").getValue().stringValue());
        s_lng = Double.parseDouble(solution.getBinding("lng").getValue().stringValue());

      }
    }

    double[] final_coords = new double[0];
    if (s_lat != -100000.00 && s_lng != -100000.00) {
      final_coords = pointAt(s_lat, s_lng, bearing, distance * 1852);
    }

    logger.debug("Creating query...");

    queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> \n";
    queryString += "PREFIX isola: <https://www.semanticweb.org/mklab/isola#> \n";
    queryString += "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n";
    queryString += "PREFIX sosa: <http://www.w3.org/ns/sosa/> \n";
    queryString += "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n";
    queryString += "DELETE { \n";
    queryString += "  ?s isola:distance ?distance . \n";
    queryString += "  ?s isola:bearing ?bearing . \n";
    queryString += "  ?s sosa:resultTime ?time . \n";
    queryString += "  ?s geo:hasGeometry ?geo . \n";
    queryString += "  ?geo geo:asWKT ?point .  \n";
    queryString += "  ?s isola:lat ?lat ; \n";
    queryString += "     isola:long ?long . \n";
    queryString += "} \n";
    queryString += "WHERE  { \n";
    queryString +=
        "    GRAPH <https://www.semanticweb.org/mklab/isola#RATTM/Info/Vessel/" + number + "> { \n";
    queryString += "        OPTIONAL { \n";
    queryString += "          ?s isola:id ?id . \n";
    queryString += "    	    ?s isola:distance ?distance . \n";
    queryString += "          ?s isola:bearing ?bearing . \n";
    queryString += "          ?s sosa:resultTime ?time . \n";
    queryString += "          ?s geo:hasGeometry ?geo . \n";
    queryString += "          ?geo geo:asWKT ?point . \n";
    queryString += "          ?s isola:lat ?lat ;  \n";
    queryString += "             isola:long ?long .  \n";
    queryString += "        } \n";
    queryString += "    } \n";
    queryString += "  FILTER (?id = " + number + ") \n";
    queryString += "}; \n";
    queryString += "INSERT { \n";
    queryString +=
        "    GRAPH <https://www.semanticweb.org/mklab/isola#RATTM/Info/Vessel/" + number + "> { \n";
    queryString += "      isola:Vessel isola:distance " + (distance * 1852.00) + " . \n";
    queryString += "      isola:Vessel isola:bearing " + bearing + " . \n";
    queryString += "      isola:Vessel isola:id " + number + " . \n";
    queryString += "			isola:Vessel sosa:resultTime " + unixTime + " . \n";
    queryString += "      isola:Vessel geo:hasGeometry [ geo:asWKT ?point] ; \n";
    queryString += "        rdf:type isola:RATTMInfo ; \n";
    queryString += "        isola:lat " + final_coords[0] + " ; \n";
    queryString += "        isola:long " + final_coords[1] + " ; \n";
    queryString += "			  sosa:resultTime " + unixTime + " . \n";
    queryString += "    } \n";
    queryString += "} WHERE { \n";
    queryString +=
        "	 BIND(CONCAT(\"POINT(\", STR(" + final_coords[0] + "), \" \", STR(" + final_coords[1]
            + "), \")\") AS ?point) \n";
    queryString += "} \n";

    logger.debug("Printing query...");
    logger.debug(queryString);

    logger.debug("Executing query...");
    Repository.executeQuery(queryString);
  }

  public void Ast1ToRdf(JSONObject object, String uuid) {
    logger.debug("In function...");

    Double x_lat = Double.parseDouble(object.get("LATITUDE").toString());
    Double y_long = Double.parseDouble(object.get("LONGITUDE").toString());
    Long userid = Long.parseLong(object.get("USERID").toString());

    /* Remove old graph before updating the information */
    IRI graph = Repository.connection
        .getValueFactory()
        .createIRI(ISOLA.NAMESPACE, "AST1/Info/Vessel/" + userid);
    Repository.connection.clear(graph);

    long unixTime = System.currentTimeMillis() / 1000L;

    logger.debug("Creating query...");

    String queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> \n";
    queryString += "PREFIX isola: <https://www.semanticweb.org/mklab/isola#> \n";
    queryString += "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n";
    queryString += "PREFIX sosa: <http://www.w3.org/ns/sosa/> \n";
    queryString += "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n";
    queryString += "INSERT { \n";
    queryString +=
        "    GRAPH <https://www.semanticweb.org/mklab/isola#AST1/Info/Vessel/" + userid + "> { \n";
    queryString += "          isola:Vessel geo:hasGeometry [ geo:asWKT ?point] ; \n";
    queryString += "              rdf:type isola:AISInfo ; \n";
    queryString += "              isola:lat " + x_lat + " ; \n";
    queryString += "              isola:long " + y_long + " ; \n";
    queryString += "			        sosa:resultTime " + unixTime + " . \n";
    queryString += "    } \n";
    queryString += "} WHERE { \n";
    queryString += "	 BIND(CONCAT(\"POINT(\", STR(" + x_lat + "), \" \", STR(" + y_long
        + "), \")\") AS ?point) \n";
    queryString += "} \n";

    logger.debug("Printing query...");
    logger.debug(queryString);

    logger.debug("Executing query...");
    Repository.executeQuery(queryString);

    queryString = "PREFIX isola: <https://www.semanticweb.org/mklab/isola#> \n";
    queryString += "PREFIX geo: <http://www.opengis.net/ont/geosparql#> \n";
    queryString += "PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/> \n";
    queryString += "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> \n";
    queryString += "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n";
    queryString += " \n";
    queryString += "SELECT ?id ?distance ?g { \n";
    queryString += "    GRAPH ?g { \n";
    queryString += "    	?s isola:id ?id . \n";
    queryString += "    	?s geo:hasGeometry [ geo:asWKT ?point1 ] . \n";
    queryString += "    	?s rdf:type isola:RATTMInfo. \n";
    queryString += "    } \n";
    queryString += "  \n";
    queryString +=
        "    GRAPH <https://www.semanticweb.org/mklab/isola#AST1/Info/Vessel/" + userid + "> { \n";
    queryString += "    	?p geo:hasGeometry [ geo:asWKT ?point2 ] . \n";
    queryString += "        ?p rdf:type isola:AISInfo . \n";
    queryString += "    } \n";
    queryString += " \n";
    queryString += "    BIND((geof:distance(?point1, ?point2, uom:metre)) as ?distance) \n";
    queryString += "} ORDER BY ASC(?distance) LIMIT 1 \n";

    TupleQuery query = Repository.connection.prepareTupleQuery(queryString);

    Integer id = -1;
    Double distance = -1.0;
    try (TupleQueryResult result = query.evaluate()) {
      // we just iterate over all solutions in the result...
      if (result.hasNext()) {
        BindingSet solution = result.next();

        /* Get the IRI of the class */
        id = Integer.parseInt(solution.getBinding("id").getValue().stringValue());
        distance = Double.parseDouble(solution.getBinding("distance").getValue().stringValue());
      }
    }

    /* Initialize RDF builder */
    ModelBuilder builder = new ModelBuilder()
        .setNamespace("isola", ISOLA.NAMESPACE);

    if (distance >= 0.0 && distance <= 300.00) {
      /* Add entity to builder */
      builder
          .namedGraph(graph)
          .subject(ISOLA.VESSEL)
          .add(ISOLA.MATCHES, id);

      /* We're done building, create our Model */
      Model model = builder.build();

      /* Commit model to repository */
      Repository.commitModel(model);

      object.put("Radar_matched_id", id.toString());
    } else {
      object.put("Radar_matched_id", "-1");
    }
  }
}
