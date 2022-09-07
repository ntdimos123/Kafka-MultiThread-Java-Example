package com.howtoprogram.kafka.auxiliary;

import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Repository {

  public static String serverURL;
  public static String graphDB;
  public static org.eclipse.rdf4j.repository.Repository repository;
  public static RepositoryConnection connection;
  public static ValueFactory f;
  public static String key;
  public static String Url;
  public static String CorId;
  public static boolean flag;

  Logger logger = LoggerFactory.getLogger(Utilities.class);

  public static void commitModel(Model model) {
    Repository.connection.begin();
    Repository.connection.add(model);
    Repository.connection.commit();
  }

  public static void executeUpdate(RepositoryConnection repositoryConnection, String update,
      Binding... bindings)
      throws MalformedQueryException, RepositoryException, UpdateExecutionException {
    Update preparedUpdate = repositoryConnection.prepareUpdate(QueryLanguage.SPARQL, update);

    /* Setting any potential bindings (query parameters) */
    for (Binding b : bindings) {
      preparedUpdate.setBinding(b.getName(), b.getValue());
    }
    preparedUpdate.execute();
  }

  public synchronized static void executeQuery(String queryString) {
    // When adding data we need to start a transaction
    Repository.connection.begin();

    executeUpdate(connection, String.format(queryString));

    Repository.connection.commit();
  }

  public void initKB() {
    logger.info("(INIT) Loading parameters and opening connection with the repository.");

    /* Load Properties */
    loadProperties();

    /* Start Repo */
    startKB(Repository.graphDB, Repository.serverURL);

    logger.info("(INIT) Initialization succeeded.");
  }

  public void loadProperties() {
    /* Read properties */
    String label = System.getenv("repositoryLabel");
    String url = System.getenv("serverURL");
    if (label == null || url == null) {
      Repository.serverURL = "http://localhost:7200";
      Repository.graphDB = "isola-test";
    } else {
      Repository.serverURL = url;
      Repository.graphDB = label;
    }
  }

  public boolean startKB(String repositoryId, String serverURL) {
    // Define server URL and appropriate credentials
    RemoteRepositoryManager manager = new RemoteRepositoryManager(serverURL);

    manager.init();

    logger.info("(REPO) Establishing connection with the repo.");
    try {
      /* Get Repository */
      Repository.repository = manager.getRepository(repositoryId);

      /* Initialize Repository */
      Repository.repository.init();

      /* Separate connection to a repository */
      Repository.connection = Repository.repository.getConnection();

      /* Initialize ValueFactory f */
      Repository.f = Repository.repository.getValueFactory();

    } catch (RepositoryConfigException | RepositoryException e) {
      logger.error("(REPO) Connecting to remote repository failed!");
      return false;
    }

    return true;
  }

  public void commitModel(Model model, Resource resource) {
    /* When adding data we need to start a transaction */
    logger.info("(REPO) Starting Transaction.");
    Repository.connection.begin();

    logger.info("(REPO) Adding Model to named graph.");
    Repository.connection.add(model, resource);

    logger.info("(REPO) Committing Transaction.");
    Repository.connection.commit();
  }

  public ModelBuilder getBuilder() {
    /* Initialize RDF builder */
    ModelBuilder builder = new ModelBuilder();

    return builder;
  }

}
