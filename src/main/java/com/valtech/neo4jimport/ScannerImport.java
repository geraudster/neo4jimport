package com.valtech.neo4jimport;

import org.neo4j.graphdb.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Created by geraud.bernonville on 19/01/14.
 */
public class ScannerImport extends Neo4jImport {
    @Override
    protected void importRating(GraphDatabaseService graphDB, Transaction tx) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(Paths.get(importDir, ratingFile).toFile()));
        String nextLine;
        Node from, to;
        Label userLabel = DynamicLabel.label("User");
        Label movieLabel = DynamicLabel.label("Movie");
        int rating;
        int count = 0;
        long startTime, endTime;
        startTime = new Date().getTime();
        Scanner scanner;
        while((nextLine = reader.readLine()) != null ) {
            scanner = new Scanner(nextLine);
            scanner.useDelimiter(separators.get("rating"));
            String userId = scanner.next();
            String movieId = scanner.next();
            String ratingValue = scanner.next();
            scanner.close();
            createRating(graphDB, userId, movieId, ratingValue);
            count++;
            if(count % 100000 == 0) {
//                tx.success();
                endTime = new Date().getTime();
                System.out.printf("%d ratings imported (elapsed: %d ms)\n", count, endTime - startTime);
                startTime = new Date().getTime();
            }
        }

        reader.close();
    }

    @Override
    protected void importMovie(GraphDatabaseService graphDB, Transaction tx) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(Paths.get(importDir, movieFile).toFile()));
        String nextLine;
        Node node;
        Label label = DynamicLabel.label("Movie");
        movies = new HashMap<>();
        Scanner scanner;
        while((nextLine = reader.readLine()) != null ) {
            scanner = new Scanner(nextLine);
            scanner.useDelimiter(separators.get("movie"));
            String movieId = scanner.next();
            String title = scanner.next();
            String releaseDate = "";
            String videoReleaseDate = "";
            System.out.printf("Importing movie [%s], id [%s]\n", title, movieId);
            scanner.close();
            createMovieNode(graphDB, label, movieId, title, releaseDate, videoReleaseDate);
        }
        tx.success();
        reader.close();
    }

    @Override
    protected void importUser(GraphDatabaseService graphDB, Transaction tx) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(Paths.get(importDir, userFile).toFile()));
        String nextLine;
        Node node;
        Label label = DynamicLabel.label("User");
        users = new HashMap<>();
        Scanner scanner;
        while((nextLine = reader.readLine()) != null ) {
            scanner = new Scanner(nextLine);
            scanner.useDelimiter(separators.get("user"));
            String userId = scanner.next();
            String gender = scanner.next();
            String age = scanner.next();
            String occupation = scanner.next();
            String zip = scanner.next();
            scanner.close();
            node = createUserNode(graphDB, label, userId, age, gender, occupation, zip);

            System.out.printf("Importing user [%s], id [%d]\n", userId, node.getId());
        }
        tx.success();

        reader.close();
    }
}
