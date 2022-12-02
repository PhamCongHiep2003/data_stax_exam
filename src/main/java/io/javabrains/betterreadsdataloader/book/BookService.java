package io.javabrains.betterreadsdataloader.book;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import javax.swing.text.html.parser.Entity;

import org.springframework.data.cassandra.core.mapping.CassandraType;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PagingState;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

import io.javabrains.betterreadsdataloader.author.Author;

import java.io.IOException;
import java.net.URL;

import com.datastax.oss.driver.api.core.cql.Row;

public class BookService {

	private static final int DPS = 10; // data page size from server
	//private static final int CPS = 5; // data page size from client
	private PagingState pageState = null;
	// private SimpleStatement st;
	private LinkedHashMap<EntityType, List<?>> dataCache;
	private SimpleStatement stAuthor;
	private SimpleStatement stBook;
	private CqlSession session;

	public static void main(String[] args) throws Exception {
		BookService bookService = new BookService();
		bookService.readData(EntityType.Book, DPS);
	}

	public BookService() throws Exception {
		dataCache = new LinkedHashMap<>();
		dataCache.put(EntityType.Author, new LinkedList<Author>());
		dataCache.put(EntityType.Book, new LinkedList<Book>());

		final String file = "secure-connect.zip";
		// "<<CLIENT ID>>"
		final String clientId = "xWWYApFRZRLxGUAkXrufjHkZ";
		// "<<CLIENT SECRET>>"
		final String clientSecret = "dxNxLJRKvIr0_Y+yE,OqWqzvR-1FsKFM1e.Ys8kfKxluo1s1K_YckN2q7lsqpvBEifmqfU7,TZu4h9tFPmG8WyLyNKy7xzGu+PZ5EHdHO5_lf1KJqs36RC8JI23.,HdE";

		URL connectBundleFile = BookService.class.getClassLoader().getResource(file);

		if (connectBundleFile == null) {
			throw new RuntimeException("Could not find or load the bundle file from the project's resources: " + file);
		}

		try {
			session = CqlSession.builder().withCloudSecureConnectBundle(connectBundleFile)
					.withAuthCredentials(clientId, clientSecret).build();
		} catch (Exception e) {
			throw (e);
		}
	}

	public void readData(EntityType entity, int dps) {

		// String query = "select id from author_by_id";
		SimpleStatement st = createQuery(entity);
		// WATCH-OUT: setPageSize returns a new SimpleStatement object!!!!!
		st = st.setPageSize(dps);
//
//		// Create the CqlSession object:
//		final String file = "secure-connect.zip";
//		// "<<CLIENT ID>>"
//		final String clientId = "xWWYApFRZRLxGUAkXrufjHkZ";
//		// "<<CLIENT SECRET>>"
//		final String clientSecret = "dxNxLJRKvIr0_Y+yE,OqWqzvR-1FsKFM1e.Ys8kfKxluo1s1K_YckN2q7lsqpvBEifmqfU7,TZu4h9tFPmG8WyLyNKy7xzGu+PZ5EHdHO5_lf1KJqs36RC8JI23.,HdE";
//
//		URL connectBundleFile = BookService.class.getClassLoader().getResource(file);
//
//		if (connectBundleFile == null) {
//			throw new RuntimeException("Could not find or load the bundle file from the project's resources: " + file);
//		}
//
//		try (CqlSession session = CqlSession.builder().withCloudSecureConnectBundle(connectBundleFile)
//				.withAuthCredentials(clientId, clientSecret).build()) {
//
//			// do {
//			// get next page and return the page state
//			pageState = doQueryNextPage(entity, session, st, pageState);
//
//			// This will be null if there are no more pages
//			// if (pageState != null) {
//			// renderNextPageLink(pageState.toString());
//			// }
//			// } while (pageState != null);
//
//		}
		
		
			// get next page and return the page state
			pageState = doQueryNextPage(entity, session, st, pageState);

			// This will be null if there are no more pages
			// if (pageState != null) {
			// renderNextPageLink(pageState.toString());
			// }
	}

	private SimpleStatement createQuery(EntityType entityType) {
		if (entityType == EntityType.Author) {
			if (stAuthor == null) {
				stAuthor = QueryBuilder.selectFrom("main", "author_by_id").columns("id", "name", "\"personalName\"")
						.build();
			}
			return stAuthor;
		} else if (entityType == EntityType.Book) {
			if (stBook == null) {
				stBook = QueryBuilder.selectFrom("main", "book_by_id").columns("book_id").build();
			}
			return stBook;
		}
		return null;
	}

	private PagingState doQueryNextPage(EntityType entityType, CqlSession session, SimpleStatement st,
			PagingState currPage) {
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
			if (entityType == EntityType.Author) {
				cacheAuthorRow(row);
			} else if (entityType == EntityType.Book) {
				cacheBookRow(row);
			}
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
	private void renderNextPageLink(String pageState) {
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
	private void cacheAuthorRow(Row row) {

		List<Author> entityCache = (List<Author>) dataCache.get(EntityType.Author);

		Author author = new Author();

		author.setId(row.getString("id"));
		// System.out.println(row.getString("name") + "\n");
		author.setName(row.getString("name"));
		// System.out.println(row.getString("\"personalName\"") + "\n");
		author.setPersonalName(row.getString("\"personalName\""));

		entityCache.add(author);

		// todo
		// System.out.printf("%s, ", row.getString("id"));
	}

	private void cacheBookRow(Row row) {

		List<Book> entityCache = (List<Book>) dataCache.get(EntityType.Book);

		Book book = new Book();

		book.setId(row.getString("book_id"));
		// book.setName(row.getString("book_name"));
		// book.setDescription(row.getString("book_description"));
		// book.setPublishedDate(row.getLocalDate("published_date"));
		// book.setAuthorIds(row.getList("authorIds", String.class));
		// book.setCoverIds(row.getList("cover_ids", String.class));
		// book.setAuthorNames(row.getList("author_names", String.class));

		entityCache.add(book);

		// todo
		System.out.printf("%s, ", row.getString("book_id"));
	}

	// get and return the next page for the Object of Specified entityType (pageSize
	// = cps)
	public List open(EntityType entityType, int cps, int pgState) {
		List<?> entityCache = dataCache.get(entityType);

		if (entityCache.size() < pgState * cps) {
			System.out.println("\n\nENTITY CACHE: " + entityCache.size() + "\n\n");

			readData(entityType, DPS);
			try {
				System.out.println("---------------------------------------------------");
				System.in.read();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("\n\nENTITY CACHE: " + entityCache.size() + "\n\n");
		}

		final List currentPage = new LinkedList<>();

		// System.out.println("\n\n");
		// System.out.println("SKIP\n");
		// System.out.println((pgState - 1) * cps);
		System.out.println("\n\n");
		System.out.println("ENTITY CACHE\n");
		System.out.println(entityCache);
		System.out.println("\n\n");

		Object[] arr = entityCache.stream().skip((pgState - 1) * cps).limit(cps).toArray();

		for (Object object : arr) {
			currentPage.add(object);
		}
		return currentPage;
	}

}
