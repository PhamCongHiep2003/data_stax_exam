package io.javabrains.betterreadsdataloader.cleaning;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.boot.SpringApplication;
// import org.springframework.boot.autoconfigure.SpringBootApplication;
// import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
// import org.springframework.boot.context.properties.EnableConfigurationProperties;
// import org.springframework.context.annotation.Bean;
// import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.json.JSONException;
import org.json.JSONObject;

import io.javabrains.betterreadsdataloader.author.Author;
import io.javabrains.betterreadsdataloader.author.AuthorRepository;
import io.javabrains.betterreadsdataloader.connection.DataStaxAstraProperties;

import java.io.FileWriter;

public class AuthorClean {

    public static void main(String[] args) {
        int startCounter = 6355000;
        String outputPath = "D:\\hoc_tap_chinh_thuc\\data structure c++\\datastax\\author_by_id.csv";
        int maxSize = 45000;
        
        String inputFile = "D:\\hoc_tap_chinh_thuc\\data structure c++\\datastax\\ol_dump_authors_2022-10-31.txt";
        clean(outputPath, inputFile, maxSize, startCounter);

    }

    public static void clean(String outputPath, String inputFile, int maxSize, int startCounter) {

        try {
            final FileWriter fw = new FileWriter(outputPath);

            Path path = Paths.get(inputFile);
            // fw.write("[\n");
            try (Stream<String> lines = Files.lines(path);) {
                final int[] counter = new int[1];
                counter[0] = 1;
                fw.write(Author.toCSVHeader());
                lines.skip(startCounter - 1).limit(maxSize).forEach(line -> {
                    String jsonString = line.substring(line.indexOf("{"));
                    try {
                        JSONObject jsonObject = new JSONObject(jsonString);

                        Author author = new Author();
                        author.setName(jsonObject.optString("name"));
                        author.setPersonalName(jsonObject.optString("personal_name"));
                        author.setId(jsonObject.optString("key").replace("/authors/", ""));

                        fw.write(author.toCSVRecord());

                        fw.write("\n");

                        System.out.println("Saving author" + author.getName() + "...");
                        counter[0]++;

                    } catch (JSONException e) {
                        e.printStackTrace();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }

                });
            }
            // fw.write("\n]");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
