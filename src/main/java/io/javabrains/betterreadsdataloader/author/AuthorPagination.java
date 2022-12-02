package io.javabrains.betterreadsdataloader.author;

import java.io.IOException;
import java.net.URL;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PagingState;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

/**
 * @overview 
 *
 * @author Duc Minh Le (ducmle)
 *
 * @version 
 */
public class AuthorPagination {

  public static void main(String[] args) {
    final int RESULTS_PER_PAGE = 10;

//    String query = "select id from author_by_id";
    SimpleStatement st = 
        QueryBuilder.selectFrom("main", "author_by_id")
        .columns("id", "name").build();
    // WATCH-OUT: setPageSize returns a new SimpleStatement object!!!!!
    st = st.setPageSize(RESULTS_PER_PAGE);
    
 // Create the CqlSession object:
    final String file = "secure-connect.zip";
	// "<<CLIENT ID>>"
	final String clientId = "xWWYApFRZRLxGUAkXrufjHkZ";
	// "<<CLIENT SECRET>>"
	final String clientSecret = "dxNxLJRKvIr0_Y+yE,OqWqzvR-1FsKFM1e.Ys8kfKxluo1s1K_YckN2q7lsqpvBEifmqfU7,TZu4h9tFPmG8WyLyNKy7xzGu+PZ5EHdHO5_lf1KJqs36RC8JI23.,HdE";

    
    URL connectBundleFile = AuthorPagination.class.getClassLoader().
        getResource(file);
    
    if (connectBundleFile == null) {
      throw new RuntimeException("Could not find or load the bundle file from the project's resources: " + file);
    }
      
    try (CqlSession session = CqlSession.builder()
        .withCloudSecureConnectBundle(connectBundleFile)
        .withAuthCredentials(clientId, clientSecret).build()) {
      PagingState pageState = null;
      
      do {
        // get next page and return the page state
        pageState = doQueryNextPage(session, st, pageState);
  
        // This will be null if there are no more pages
        if (pageState != null) {
          renderNextPageLink(pageState.toString());
        }
      } while (pageState != null);
      
    }
    
  }

  private static PagingState doQueryNextPage(CqlSession session, SimpleStatement st, PagingState currPage) {
    System.out.println("keyspace: " + st.getKeyspace());
    System.out.println("pageSize: " + st.getPageSize());

    ResultSet rs = session.execute(st);
    PagingState nextPage = null;
    if (currPage != null) {
      st.setPagingState(currPage);
    }
    
    nextPage = rs.getExecutionInfo().getSafePagingState();

    // Note that we don't rely on RESULTS_PER_PAGE, since Cassandra might
    // have not respected it, or we might be at the end of the result set
    int remaining = rs.getAvailableWithoutFetching();
    for (Row row : rs) {
        renderInResponse(row);
        if (--remaining == 0) {
            break;
        }
    }
    
    System.out.println("");
    return nextPage;
  }
  /**
   * @effects 
   * 
   * @version 
   * 
   */
  private static void renderNextPageLink(String pageState) {
    System.out.println("===> " + pageState);
    try {
      System.in.read();
    } catch (IOException e) {
      // ignore;
    }
  }

  /**
   * @effects 
   * 
   * @version 
   * 
   */
  private static void renderInResponse(Row row) {
    // todo
    System.out.printf("%s, ", row.getString("id"));
  }
}
