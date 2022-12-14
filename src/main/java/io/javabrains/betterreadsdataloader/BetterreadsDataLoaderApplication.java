// package io.javabrains.betterreadsdataloader;

// import java.io.IOException;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.nio.file.Paths;
// import java.time.LocalDate;
// import java.time.format.DateTimeFormatter;
// import java.util.ArrayList;
// import java.util.List;
// import java.util.stream.Collectors;
// import java.util.stream.Stream;

// import javax.annotation.PostConstruct;

// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.boot.SpringApplication;
// import org.springframework.boot.autoconfigure.SpringBootApplication;
// import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
// import org.springframework.boot.context.properties.EnableConfigurationProperties;
// import org.springframework.context.annotation.Bean;
// import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
// import org.json.JSONArray;
// import org.json.JSONException;
// import org.json.JSONObject;

// import io.javabrains.betterreadsdataloader.author.Author;
// import io.javabrains.betterreadsdataloader.author.AuthorRepository;
// import io.javabrains.betterreadsdataloader.book.Book;
// import io.javabrains.betterreadsdataloader.book.BookRepository;
// import io.javabrains.betterreadsdataloader.connection.DataStaxAstraProperties;

// @SpringBootApplication
// @EnableConfigurationProperties(DataStaxAstraProperties.class)
// @EnableCassandraRepositories
// public class BetterreadsDataLoaderApplication {

// 	@Autowired
// 	AuthorRepository authorRepository;

// 	@Autowired
// 	BookRepository bookRepository;

// 	public static void main(String[] args) {
// 		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
// 	}

// 	@Value("${datadump.location.author}")
// 	private String authorDumpLocation;

// 	@Value("${datadump.location.works}")
// 	private String worksDumpLocation;

// 	@PostConstruct
// 	public void start() {
// 		// initAuthors();
// 		initWorks();
// 	}

// 	private void initWorks() {
// 		Path path = Paths.get(worksDumpLocation);
// 		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		
// 		try (Stream<String> lines = Files.lines(path);) {
// 			lines.forEach(line -> {
// 				String jsonString = line.substring(line.indexOf("{"));
// 				try {
// 					JSONObject jsonObject = new JSONObject(jsonString);

// 					Book book = new Book();
// 					book.setId(jsonObject.getString("key").replace("/works/", ""));
// 					book.setName(jsonObject.optString("title"));

// 					JSONObject descriptionObj = jsonObject.optJSONObject("description");
// 					if (descriptionObj != null) {
// 						book.setDescription(descriptionObj.optString("value"));
// 					}

// 					JSONObject publishedObj = jsonObject.optJSONObject("created");
// 					if (publishedObj != null) {
// 						String dateStr = publishedObj.getString("value");
// 						book.setPublishedDate(LocalDate.parse(dateStr, dateFormat));
// 					}

// 					// JSONArray coversJSONArr = jsonObject.optJSONArray("covers");
// 					// if (coversJSONArr != null) {
// 					// 	List<String> coverIds = new ArrayList<>();
// 					// 	for (int i = 0; i < coversJSONArr.length(); i++) {
// 					// 		coverIds.add(coversJSONArr.getString(i));
// 					// 	}
// 					// 	book.setCoverIds(coverIds);
// 					// }

// 					JSONArray authorsJSONArr = jsonObject.optJSONArray("authors");
// 					if (authorsJSONArr != null) {
// 						List<String> authorIds = new ArrayList<>();
// 						for (int i = 0; i < authorsJSONArr.length(); i++) {
// 							String authorId = authorsJSONArr.getJSONObject(i).getJSONObject("author")
// 									.getString("key")
// 									.replace("/authors/", "");
// 							authorIds.add(authorId);
// 						}
// 						book.setAuthorIds(authorIds);
// 					}
// 					fw.write(author.toCSVRecord());

//                         fw.write("\n");

// 					System.out.println("Saving book" + book.getName() + "...");
// 					bookRepository.save(book);

// 				} catch (Exception e) {
// 					e.printStackTrace();
// 				}
// 			});
// 		} catch (Exception e) {
// 			e.printStackTrace();
// 		}
// 	}

// 	// private void initAuthors() {
// 	// Path path = Paths.get(authorDumpLocation);
// 	// try (Stream<String> lines = Files.lines(path);) {
// 	// lines.limit(1).forEach(line -> {
// 	// String jsonString = line.substring(line.indexOf("{"));
// 	// try {
// 	// JSONObject jsonObject = new JSONObject(jsonString);

// 	// Author author = new Author();
// 	// author.setName(jsonObject.optString("name"));
// 	// author.setPersonalName(jsonObject.optString("personal_name"));
// 	// author.setId(jsonObject.optString("key").replace("/authors", ""));

// 	// System.out.println("Saving author" + author.getName() + "...");
// 	// authorRepository.save(author);
// 	// } catch (JSONException e) {
// 	// e.printStackTrace();
// 	// }

// 	// });
// 	// } catch (IOException e) {
// 	// e.printStackTrace();
// 	// }
// 	// }

// 	@Bean
// 	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
// 		Path bundle = astraProperties.getSecureConnectBundle().toPath();
// 		return builder -> builder.withCloudSecureConnectBundle(bundle);
// 	}

// }
