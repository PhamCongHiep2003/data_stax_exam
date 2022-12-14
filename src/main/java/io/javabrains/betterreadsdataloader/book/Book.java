package io.javabrains.betterreadsdataloader.book;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.CassandraType.Name;

/**
 * Model that represents the books_by_id table in Cassandra.
 * Stores the book information retrievable by the book ID
 */

@Table(value = "book_by_id")
public class Book {
    
    @Id @PrimaryKeyColumn(name = "book_id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    private String id;
    
    @Column("book_name")
    @CassandraType(type = Name.TEXT)
    private String name;

    @Column("book_description")
    @CassandraType(type = Name.TEXT)
    private String description;

    @Column("published_date")
    @CassandraType(type = Name.DATE)
    private LocalDate publishedDate;

    // @Column("cover_ids")
    // @CassandraType(type = Name.LIST, typeArguments = Name.TEXT)
    // private List<String> coverIds;

    // @Column("author_names")
    // @CassandraType(type = Name.LIST, typeArguments = Name.TEXT)
    // private List<String> authorNames;

    @Column("author_id")
    @CassandraType(type = Name.LIST, typeArguments = Name.TEXT)
    private List<String> authorIds;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public LocalDate getPublishedDate() {
        return publishedDate;
    }

    public void setPublishedDate(LocalDate publishedDate) {
        this.publishedDate = publishedDate;
    }



    public List<String> getAuthorIds() {
        return authorIds;
    }

    public void setAuthorIds(List<String> authorIds) {
        this.authorIds = authorIds;
    }
    
    // public String toJson() {

    //     String json = "{\"id\": \"%s\", \"name\": \"%s\", \"personalName\": \"%s\"}";
    //     json = String.format(json, id, name, personalName);
    //     return json;
    // }

    public String toAuthorIdsString() {
        String a ="[";
        for(int i=0; i<=authorIds.size()-2; i++) {
            a += authorIds.get(i) + ",";
        }
        a += authorIds.get(authorIds.size()-1) + "]";
        return a;
    }

    public String toDate() {
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd"); 
        if(this.publishedDate == null) {
            return "";
        } 
        String strDate = this.publishedDate.format(dateFormat);  
        return strDate;
    }
    public String toCSVRecord() {

        String CSV = "%s,\"%s\",\"%s\",\"%s\",\"%s\"";
        CSV = String.format(CSV, id, name, description, toDate(), toAuthorIdsString());
        return CSV;
    }
    
    public static String toCSVHeader() {
        return "book_id,book_name,book_description,published_date,author_id\n";
    }

}
