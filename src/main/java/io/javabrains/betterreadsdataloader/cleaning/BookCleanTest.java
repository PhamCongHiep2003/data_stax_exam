// package io.javabrains.betterreadsdataloader.cleaning;

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

// import org.json.JSONArray;
// // import org.springframework.beans.factory.annotation.Autowired;
// // import org.springframework.beans.factory.annotation.Value;
// // import org.springframework.boot.SpringApplication;
// // import org.springframework.boot.autoconfigure.SpringBootApplication;
// // import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
// // import org.springframework.boot.context.properties.EnableConfigurationProperties;
// // import org.springframework.context.annotation.Bean;
// // import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
// import org.json.JSONException;
// import org.json.JSONObject;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.format.datetime.DateFormatter;

// import io.javabrains.betterreadsdataloader.author.Author;
// import io.javabrains.betterreadsdataloader.author.AuthorRepository;
// import io.javabrains.betterreadsdataloader.book.Book;
// import io.javabrains.betterreadsdataloader.book.BookRepository;
// import io.javabrains.betterreadsdataloader.connection.DataStaxAstraProperties;

// import java.io.FileWriter;

// public class BookCleanTest {
    
//     @Autowired
//     AuthorRepository authorRepository;

//     @Autowired
//     BookRepository bookRepository;

//     public static void main(String[] args) {
        

//     }

//     @PostConstruct
//     public void start() {
//         int startCounter = 1;
//         String outputPath = "D:\\hoc_tap_chinh_thuc\\data structure c++\\datastax\\book_by_id.txt";
//         int maxSize = 10;

//         String inputFile = "D:\\hoc_tap_chinh_thuc\\data structure c++\\datastax\\ol_dump_works_2022-10-31.txt";
//         clean(outputPath, inputFile, maxSize, startCounter);
//     }

//     public static void clean(String outputPath, String inputFile, int maxSize, int startCounter) {

//         try {
//             final FileWriter fw = new FileWriter(outputPath);

//             Path path = Paths.get(inputFile);
//             // fw.write("[\n");
//             DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
//             try (Stream<String> lines = Files.lines(path);) {
//                 final int[] counter = new int[1];
//                 counter[0] = 1;
//                 //fw.write(Author.toCSVHeader());
//                 lines.skip(startCounter - 1).limit(maxSize).forEach(line -> {
//                     String jsonString = line.substring(line.indexOf("{"));
//                     try {
//                         JSONObject jsonObject = new JSONObject(jsonString);

//                         Book book = new Book();
//                         book.setId(jsonObject.getString("key").replace("/works/", ""));
//                         ;
//                         book.setName(jsonObject.optString("title"));

//                         JSONObject descriptionObj = jsonObject.optJSONObject("description");
//                         if (descriptionObj != null) {
//                             book.setDescription(descriptionObj.optString("value"));
//                         }

//                         JSONObject publishedObj = jsonObject.optJSONObject("created");
//                         if (publishedObj != null) {
//                             String dateStr = publishedObj.getString("value");
//                             book.setPublishedDate(LocalDate.parse(dateStr, dateFormat));
//                         }

//                         JSONArray coversJSONArr = jsonObject.optJSONArray("covers");
//                         if (coversJSONArr != null) {
//                             List<String> coverIds = new ArrayList<>();
//                             for (int i = 0; i < coversJSONArr.length(); i++) {
//                                 coverIds.add(coversJSONArr.getString(i));
//                             }
//                             book.setCoverIds(coverIds);
//                         }

//                         JSONArray authorsJSONArr = jsonObject.optJSONArray("authors");
//                         if (authorsJSONArr != null) {
//                             List<String> authorIds = new ArrayList<>();
//                             for (int i = 0; i < authorsJSONArr.length(); i++) {
//                                 String authorId = authorsJSONArr.getJSONObject(i).getJSONObject("author")
//                                         .getString("key")
//                                         .replace("/authors/", "");
//                                 authorIds.add(authorId);
//                             }
//                             book.setAuthorIds(authorIds);
//                             List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
//                                     .map(optionalAuthor -> {
//                                         if (!optionalAuthor.isPresent())
//                                             return "Unknown Author";
//                                         return optionalAuthor.get().getName();
//                                     }).collect(Collectors.toList());

//                             book.setAuthorNames(authorNames);
//                         }

//                         //fw.write(book.toCSVRecord());

//                         fw.write("\n");

//                         System.out.println("Saving book" + book.getName() + "...");
//                         bookRepository.save(book);
//                         counter[0]++;

//                     } catch (JSONException e) {
//                         e.printStackTrace();
//                     } catch (IOException e1) {
//                         e1.printStackTrace();
//                     }

//                 });
//             }
//             // fw.write("\n]");
//             fw.close();
//         } catch (IOException e) {
//             e.printStackTrace();
//         }
//     }
// }
