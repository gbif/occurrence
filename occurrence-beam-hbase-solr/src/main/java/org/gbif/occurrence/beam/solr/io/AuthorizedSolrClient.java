package org.gbif.occurrence.beam.solr.io;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Closeable;
import java.io.IOException;


import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.SolrParams;

/**
 * Client for interact with Solr.
 * @param <ClientT> type of SolrClient
 */
class AuthorizedSolrClient<ClientT extends SolrClient> implements Closeable {
  private final ClientT solrClient;
  private final String username;
  private final String password;

  AuthorizedSolrClient(ClientT solrClient, PatchedSolrIO.ConnectionConfiguration configuration) {
    checkArgument(
        solrClient != null,
        "solrClient can not be null");
    checkArgument(
        configuration != null,
        "configuration can not be null");
    this.solrClient = solrClient;
    this.username = configuration.getUsername();
    this.password = configuration.getPassword();
  }

  QueryResponse query(String collection, SolrParams solrParams)
      throws IOException, SolrServerException {
    QueryRequest query = new QueryRequest(solrParams);
    return process(collection, query);
  }

  <ResponseT extends SolrResponse> ResponseT process(String collection,
                                                     SolrRequest<ResponseT> request) throws IOException, SolrServerException {
    request.setBasicAuthCredentials(username, password);
    return request.process(solrClient, collection);
  }

  CoreAdminResponse process(CoreAdminRequest request)
      throws IOException, SolrServerException {
    return process(null, request);
  }

  SolrResponse process(CollectionAdminRequest request)
      throws IOException, SolrServerException {
    return process(null, request);
  }

  static ClusterState getClusterState(
      AuthorizedSolrClient<CloudSolrClient> authorizedSolrClient) {
    authorizedSolrClient.solrClient.connect();
    return authorizedSolrClient.solrClient.getZkStateReader().getClusterState();
  }

  @Override public void close() throws IOException {
    solrClient.close();
  }
}
