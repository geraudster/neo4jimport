package com.valtech.neo4jimport;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by geraud.bernonville on 17/01/14.
 */
public abstract class Neo4jImport {


    protected static final int RATING_USER_ID = 0;
    protected static final int RATING_MOVIE_ID = 1;
    protected static final int RATING_VALUE = 2;
    protected static final int MOVIE_ID=0;
    protected static final int TITLE=1;
    protected static final int RELEASE_DATE=2;
    protected static final int VIDEO_RELEASE_DATE=3;
    protected static final int USER_ID=0;
    protected static final int AGE=1;
    protected static final int GENDER=2;
    protected static final int OCCUPATION=3;
    protected static final int ZIP=4;
    @Parameter(names = "-dbPath", description = "Path to Neo4j data directory")
    public String dbPath;
    @Parameter(names =  "-importDir", description = "Path to dataset directory")
    public String importDir;
    @DynamicParameter(names = "-S", description = "Override separators")
    public Map<String, String> separators;
    @Parameter(names = "-hasHeader", description = "Tell if datasets have header")
    public boolean hasHeader;
    @Parameter(names = "-u")
    public String userFile = "u.user";
    @Parameter(names = "-m")
    public String movieFile = "u.item";
    @Parameter(names = "-r")
    public String ratingFile = "u.data";
    protected Map<String, Node> movies;
    protected Map<String, Node> users;
    public Neo4jImport() {
        separators = new HashMap<>();
        separators.put("user", "|");
        separators.put("movie", "|");
        separators.put("rating", "\t");
    }

    public static void main(String args[]) throws IOException {
//        String dbPath = args[0];
//        Path importDir = Paths.get(args[1]);
        Neo4jImport neo4jimport = new ScannerImport();
        long start = new Date().getTime();
//        Neo4jImport neo4jimport = new CsvImport();
        new JCommander(neo4jimport, args);
        System.out.println(neo4jimport.separators);
        if(Paths.get(neo4jimport.dbPath).toFile().isDirectory() && Paths.get(neo4jimport.dbPath, "neostore").toFile().canWrite()) {
            System.out.printf("Clearing db [%s]\n", neo4jimport.dbPath);
            neo4jimport.clearDB();
        }
        neo4jimport.insert();
        long end = new Date().getTime();
        System.out.printf("Importing all in %d ms\n", end - start);
    }

    public void clearDB() throws IOException {
        Path path = Paths.get(dbPath);
        Files.walkFileTree(path, new SimpleFileVisitor<Path>(){
            @Override
            public FileVisitResult visitFile(Path file,
                                             BasicFileAttributes attrs) throws IOException {

                System.out.println("Deleting file: " + file);
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir,
                                                      IOException exc) throws IOException {

                System.out.println("Deleting dir: " + dir);
                if (exc == null) {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                } else {
                    throw exc;
                }
            }
        });
    }

    public void insert() throws IOException {
//        Map<String, String> config = new HashMap<>();
//        config.put("neostore.nodestore.db.mapped_memory", "180M");
//        config.put("neostore.relationshipstore.db.mapped_memory", "2G");
//        config.put("neostore.propertystore.db.mapped_memory", "50M");
//        config.put("neostore.propertystore.db.strings.mapped_memory", "100M");
//        config.put("neostore.propertystore.db.arrays.mapped_memory", "0M");
//        BatchInserter inserter = BatchInserters.
//        GraphDatabaseService graphDB = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath).newGraphDatabase();
        GraphDatabaseService graphDB = BatchInserters.batchDatabase(dbPath);
        registerShutdownHook( graphDB );
        users = new HashMap<>();
        long startTime = new Date().getTime();
        try (Transaction tx = graphDB.beginTx()) {
//            importUser(graphDB, tx);
            importMovie(graphDB, tx);
            importRating(graphDB, tx);
            tx.success();
        }
        long endTime = new Date().getTime();
        System.out.printf("Import done in %d ms\n", endTime - startTime);
    }

//    protected String getGenre(String[] nextLine) {
//
//
//    }

    protected void registerShutdownHook(final GraphDatabaseService graphDB) {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDB.shutdown();
            }
        } );
    }

    protected abstract void importRating(GraphDatabaseService graphDB, Transaction tx) throws IOException;

    protected void createRating(GraphDatabaseService graphDB, String userId, String movieId, String ratingValue) {
        Node from;
        Node to;
        int rating;
        from = users.get(userId);
        if(from == null) {
            Label userLabel = DynamicLabel.label("User");
            from = graphDB.createNode(userLabel);
            from.setProperty("user_id", userId);
            users.put(userId, from);
        }
        to = movies.get(movieId);
        rating = (int)Float.parseFloat(ratingValue);
        Relationship relationship = from.createRelationshipTo(to, RelTypes.HAS_RATED);
        relationship.setProperty("value", rating);
    }

    protected abstract void importMovie(GraphDatabaseService graphDB, Transaction tx) throws IOException;

    protected void createMovieNode(GraphDatabaseService graphDB, Label label, String movieId, String title, String releaseDate, String videoReleaseDate) {
        Node node;
        node = graphDB.createNode(label);
        node.setProperty("movie_id", movieId);
        node.setProperty("title", title);
        node.setProperty("release_date", releaseDate);
        node.setProperty("video_release_date", videoReleaseDate);
        movies.put(movieId, node);
    }

    protected abstract void importUser(GraphDatabaseService graphDB, Transaction tx) throws IOException;

    protected Node createUserNode(GraphDatabaseService graphDB, Label label, String userId, String age, String gender, String occupation, String zip) {
        Node node;
        node = graphDB.createNode(label);
        node.setProperty("user_id", userId);
        node.setProperty("age", age);
        node.setProperty("gender", gender);
        node.setProperty("occupation", occupation);
        node.setProperty("zip", zip);
        users.put(userId, node);
        return node;
    }

    protected static enum RelTypes implements RelationshipType {
        HAS_RATED, IS_FRIEND_OF
    }
}
