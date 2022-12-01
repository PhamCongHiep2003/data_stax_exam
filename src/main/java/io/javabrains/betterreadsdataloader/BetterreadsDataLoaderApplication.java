package io.javabrains.betterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.json.JSONException;
import org.json.JSONObject;

import io.javabrains.betterreadsdataloader.author.Author;
import io.javabrains.betterreadsdataloader.author.AuthorRepository;
import io.javabrains.betterreadsdataloader.connection.DataStaxAstraProperties;


@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
@EnableCassandraRepositories("io.javabrains.betterreadsdataloader.author")
public class BetterreadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;
	
	// @PostConstruct
	// public void start() {
	// 	initAuthors();
	// 	initWorks();
	// }

	// private void initWorks() {
	// }


	// private void initAuthors() {
	// 	Path path = Paths.get(authorDumpLocation);
	// 	try (Stream<String> lines = Files.lines(path);){
	// 		lines.limit(1).forEach(line -> {
	// 			String jsonString = line.substring(line.indexOf("{"));
	// 			try {
	// 				JSONObject jsonObject = new JSONObject(jsonString);

	// 			Author author = new Author();
	// 			author.setName(jsonObject.optString("name"));
	// 			author.setPersonalName(jsonObject.optString("personal_name"));
	// 			author.setId(jsonObject.optString("key").replace("/authors", ""));
				
	// 			System.out.println("Saving author" + author.getName() + "...");
	// 			authorRepository.save(author);
	// 			return;
	// 			} catch (JSONException e) {
	// 				e.printStackTrace();
	// 			}
				
	// 		});
	// 	} catch (IOException e) {
	// 		e.printStackTrace();
	// 	}
	// }


	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}
