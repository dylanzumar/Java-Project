import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

public class predictSpringUpload {

    // Provide mySQL server, database name, table name, username, password, path to data, and path to schema
    static String url = "jdbc:mysql://localhost:3306/";
    static String database = "predictSpring";
    static String tableName = "Product";
    static String username = "dylanzumar";
    static String password = "password";
    static String pathToData = "./Product_feed.tsv";
    static String pathToSchema = "./productFeedSchema.sql";

    // Creates desired database and creates table based on schema provided (assumes user has permissions to create database and grant itself privileges)
    public static void createTable(Connection connection, String url, String database, String username, String password) {
        try {
            // Create database
            Statement statement = connection.createStatement();
            String createDatabase = "CREATE DATABASE IF NOT EXISTS " + database + ";";
            statement.executeUpdate(createDatabase);
            // Use database
            String useDatabase = "USE " + database + ";";
            statement.executeUpdate(useDatabase);
            // Grant user privileges
            String grantPermissions = String.format("GRANT ALL ON %s.* TO '%s'@'localhost';", database, username);
            statement.executeUpdate(grantPermissions);
            // Create table
            String createTable = Files.readString(Paths.get(pathToSchema));
            statement.executeUpdate(createTable);

            System.out.println("Table upload succeeded");
        }

        catch (Exception e) {
            System.out.println("Table upload failed");
            e.printStackTrace();
        }
    }

    // Converts TSV file to list of strings
    public static List<String[]> parseTSV(String fileName) {
        List<String[]> data = null;
        try {
            TsvParserSettings settings = new TsvParserSettings();
            // Assume that rows are separated by line
            settings.getFormat().setLineSeparator("\n");
            TsvParser parser = new TsvParser(settings);
            data = parser.parseAll(new File(pathToData));
        }

        catch (Exception e) {
            System.out.println("Failed to parse TSV");
            e.printStackTrace();
        }
        return data;
    }

    // Inserts each row in list into desired table
    public static void insertDataToTable(Connection connection, List<String[]> data) {
        for (int row = 0; row < data.size(); row++) {
            // Create format for row insertion
            String insertString = String.format("insert into %s values (", tableName);
            if (row != 0) {
                for (String dataPoint : Arrays.asList(data.get(row))) {
                    // If row is an empty string, treat it as null
                    if (dataPoint != null && dataPoint.equals("") == true)
                        dataPoint = null;
                    // Add escape characters to apostrophes
                    if (dataPoint != null && dataPoint.indexOf("'") != -1)
                        dataPoint = dataPoint.replaceAll("\\'", "\\\\'");
                    // Replace quotation marks with properly-escaped apostrophes
                    if (dataPoint != null && dataPoint.indexOf('"') != -1)
                        dataPoint = dataPoint.replace("\"", "\\\'");
                    // If data is not a boolean or null, put quotes around it
                    if (dataPoint != null && dataPoint.equals("true") == false && dataPoint.equals("false") == false)
                        insertString += '"' + dataPoint + '"' + ",";
                    // If boolean or null, add to the list unmodified
                    else
                        insertString += dataPoint + ",";
                }
                // Close off the list in order to insert row
                insertString = insertString.substring(0, insertString.length() - 1) + ");";
                try {
                    Statement statement = connection.createStatement();
                    // Insert row
                    statement.executeUpdate(insertString);
                    System.out.println("Succeeded in inserting row: " + String.join(", ", Arrays.asList(data.get(row))));
                }

                catch (Exception e) {
                    // If primary key already exists, skip this row
                    if (e.getMessage().indexOf("Duplicate entry") != -1)
                        System.out.println("Duplicate row, skipping.");
                    else {
                        System.out.println("Failed to insert row: " + String.join(", ", Arrays.asList(data.get(row))));
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        Connection connection = null;
        try {
            // Start connection to mySQL
            connection = DriverManager.getConnection(url + database, username, password);
        }

        catch (Exception e) {
            System.out.println("Connection failed to start");
            e.printStackTrace();
        }
        // Create table based on schema
        createTable(connection, url, database, username, password);
        // Parse data to be added to table
        List<String[]> data = parseTSV(pathToData);
        // Insert the parsed data to table
        insertDataToTable(connection, data);

        try {
            // Close mySQL connection
            connection.close();
        }

        catch (Exception e) {
            System.out.println("Connection failed to close");
            e.printStackTrace();
        }
    }
}