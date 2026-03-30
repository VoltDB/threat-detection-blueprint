package com.example.voltdb;

import org.voltdb.client.Client2;
import org.voltdb.client.ClientResponse;
import org.voltdb.VoltTable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * One-time schema deployment utility.
 * Checks whether schema is already deployed via @SystemCatalog.
 * If not, loads procedure classes and executes DDL.
 */
public class VoltDBSetup {

    private static final String JAR_PATH = System.getProperty(
            "procedures.jar.path", "target/threat-detection-blueprint-1.0-procedures.jar");
    private static final String DDL_RESOURCE = "voltdb-ddl.sql";

    private final Client2 client;

    public VoltDBSetup(Client2 client) {
        this.client = client;
    }

    public void initSchemaIfNeeded() throws Exception {
        if (isSchemaDeployed()) {
            System.out.println("Schema already deployed — redeploying procedures.");
            redeployProcedures();
            return;
        }

        loadClassesAndDdl();
    }

    private void loadClassesAndDdl() throws Exception {
        File jarFile = new File(JAR_PATH);
        if (!jarFile.exists()) {
            throw new RuntimeException(
                "Jar not found: " + JAR_PATH + ". Run 'mvn package -DskipTests' first.");
        }

        System.out.println("Loading classes from: " + jarFile);
        byte[] jarBytes = Files.readAllBytes(jarFile.toPath());
        ClientResponse response = client.callProcedureSync("@UpdateClasses", jarBytes, null);
        if (response.getStatus() != ClientResponse.SUCCESS) {
            throw new RuntimeException("Failed to load classes: " + response.getStatusString());
        }
        System.out.println("Classes loaded successfully.");

        String ddl = loadResourceAsString(DDL_RESOURCE);
        if (ddl == null) {
            throw new RuntimeException("DDL resource not found: " + DDL_RESOURCE);
        }
        System.out.println("Loading schema from classpath: " + DDL_RESOURCE);
        response = client.callProcedureSync("@AdHoc", ddl);
        if (response.getStatus() != ClientResponse.SUCCESS) {
            throw new RuntimeException("DDL failed: " + response.getStatusString());
        }
        System.out.println("Schema deployment complete.");
    }

    /**
     * Drop and recreate Java stored procedures so that updated procedure code
     * takes effect without a full schema redeploy or VoltDB restart.
     */
    private void redeployProcedures() throws Exception {
        File jarFile = new File(JAR_PATH);
        if (!jarFile.exists()) {
            throw new RuntimeException(
                "Jar not found: " + JAR_PATH + ". Run 'mvn package -DskipTests' first.");
        }

        // Drop existing Java procedures
        String dropDdl =
            "DROP PROCEDURE com.example.voltdb.procedures.ProcessTransaction IF EXISTS;\n" +
            "DROP PROCEDURE com.example.voltdb.procedures.RecordSubnetRequest IF EXISTS;";
        client.callProcedureSync("@AdHoc", dropDdl);
        System.out.println("Dropped existing Java procedures.");

        // Reload classes from the latest JAR
        byte[] jarBytes = Files.readAllBytes(jarFile.toPath());
        ClientResponse response = client.callProcedureSync("@UpdateClasses", jarBytes, null);
        if (response.getStatus() != ClientResponse.SUCCESS) {
            throw new RuntimeException("Failed to reload classes: " + response.getStatusString());
        }

        // Recreate procedures
        String createDdl =
            "CREATE PROCEDURE PARTITION ON TABLE TRANSACTIONS COLUMN ACCOUNT_ID " +
            "FROM CLASS com.example.voltdb.procedures.ProcessTransaction;\n" +
            "CREATE PROCEDURE PARTITION ON TABLE SUBNET_REQUESTS COLUMN SUBNET " +
            "FROM CLASS com.example.voltdb.procedures.RecordSubnetRequest;";
        response = client.callProcedureSync("@AdHoc", createDdl);
        if (response.getStatus() != ClientResponse.SUCCESS) {
            throw new RuntimeException("Failed to recreate procedures: " + response.getStatusString());
        }
        System.out.println("Procedures redeployed successfully.");
    }

    private boolean isSchemaDeployed() throws Exception {
        // Check for both a table and a procedure to ensure schema is fully loaded
        boolean hasTable = false;
        ClientResponse response = client.callProcedureSync("@SystemCatalog", "TABLES");
        VoltTable tables = response.getResults()[0];
        while (tables.advanceRow()) {
            if ("ACCOUNTS".equalsIgnoreCase(tables.getString("TABLE_NAME"))) {
                hasTable = true;
                break;
            }
        }
        if (!hasTable) {
            return false;
        }

        boolean hasProc = false;
        response = client.callProcedureSync("@SystemCatalog", "PROCEDURES");
        VoltTable procs = response.getResults()[0];
        while (procs.advanceRow()) {
            if ("RecordSubnetRequest".equalsIgnoreCase(procs.getString("PROCEDURE_NAME"))) {
                hasProc = true;
                break;
            }
        }
        return hasProc;
    }

    private String loadResourceAsString(String resourcePath) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                System.err.println("Resource not found: " + resourcePath);
                return null;
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read resource: " + resourcePath, e);
        }
    }
}