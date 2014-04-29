package com.valtech.neo4jimport;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import org.neo4j.graphdb.*;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by geraud.bernonville on 19/01/14.
 */
public class CsvImport extends Neo4jImport {
    protected void importRating(GraphDatabaseService graphDB, Transaction tx) throws IOException {
        CSVReader reader = new CSVReader(new FileReader(Paths.get(importDir, ratingFile).toFile()), separators.get("rating").charAt(0), CSVParser.DEFAULT_QUOTE_CHARACTER, hasHeader ? 1 : 0);
        String[] nextLine;
        Node from, to;
        Label userLabel = DynamicLabel.label("User");
        Label movieLabel = DynamicLabel.label("Movie");
        int rating;
        int count = 0;
        long startTime, endTime;
        startTime = new Date().getTime();
        while((nextLine = reader.readNext()) != null ) {
            String userId = nextLine[RATING_USER_ID];
            String movieId = nextLine[RATING_MOVIE_ID];
            String ratingValue = nextLine[RATING_VALUE];
            createRating(graphDB, userId, movieId, ratingValue);
            count++;
            if(count % 5000 == 0) {
                tx.success();
                endTime = new Date().getTime();
                System.out.printf("%d ratings imported (elapsed: %d ms)\n", count, endTime - startTime);
                startTime = new Date().getTime();
            }
        }
    }

    protected void importMovie(GraphDatabaseService graphDB, Transaction tx) throws IOException {
        CSVReader reader = new CSVReader(new FileReader(Paths.get(importDir, movieFile).toFile()), separators.get("movie").charAt(0), CSVParser.DEFAULT_QUOTE_CHARACTER, hasHeader ? 1 : 0);
        String[] nextLine;
        Node node;
        Label label = DynamicLabel.label("Movie");
        movies = new HashMap<>();
        while((nextLine = reader.readNext()) != null ) {
            String movieId = nextLine[MOVIE_ID];
            String title = nextLine[TITLE];
            String releaseDate = nextLine[RELEASE_DATE];
            String videoReleaseDate = nextLine[VIDEO_RELEASE_DATE];
            createMovieNode(graphDB, label, movieId, title, releaseDate, videoReleaseDate);
//            node.setProperty("genre", getGenre(nextLine));
            System.out.printf("Importing movie [%s], id [%s]\n", title, movieId);
        }
        tx.success();
    }

    protected void importUser(GraphDatabaseService graphDB, Transaction tx) throws IOException {
        CSVReader reader = new CSVReader(new FileReader(Paths.get(importDir, userFile).toFile()), separators.get("user").charAt(0), CSVParser.DEFAULT_QUOTE_CHARACTER, hasHeader ? 1 : 0);
        String[] nextLine;
        Node node;
        Label label = DynamicLabel.label("User");
        users = new HashMap<>();

        while((nextLine = reader.readNext()) != null ) {
            String userId = nextLine[USER_ID];
            String age = nextLine[AGE];
            String gender = nextLine[GENDER];
            String occupation = nextLine[OCCUPATION];
            String zip = nextLine[ZIP];
            node = createUserNode(graphDB, label, userId, age, gender, occupation, zip);

            System.out.printf("Importing user [%s], id [%d]\n", userId, node.getId());
        }
        tx.success();
    }
}
