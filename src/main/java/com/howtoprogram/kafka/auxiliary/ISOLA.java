package com.howtoprogram.kafka.auxiliary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class ISOLA {
  public static final String NAMESPACE = "https://www.semanticweb.org/mklab/isola#";

  public static final String PREFIX = "isola";

  public static final IRI TIME = getIRI("Timestamp");

  public static final IRI VESSEL = getIRI("Vessel");

  public static final IRI MATCHES = getIRI("matchesVessel");

  /**
   * Creates a new {@link IRI} with this vocabulary's namespace for the given local name.
   *
   * @param localName a local name of an IRI, e.g. 'creatorOf', 'name', 'Artist', etc.
   * @return an IRI using the http://www.semanticweb.org/image-ontology/ namespace and the given local name.
   */
  private static IRI getIRI(String localName) {
    return SimpleValueFactory.getInstance().createIRI(NAMESPACE, localName);
  }
}
