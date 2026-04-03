/* This file is part of VoltDB.
 * Copyright (C) 2026 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package com.example.voltdb;

import org.voltdb.client.Client2;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdbtest.testcontainer.VoltDBCluster;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IntegrationTestBase {

    private static final Properties props = new Properties();
    static {
        try (InputStream input = IntegrationTestBase.class.getClassLoader()
                .getResourceAsStream("test.properties")) {
            if (input != null) {
                props.load(input);
            }
        } catch (IOException e) {
            // Use defaults
        }
    }

    public String getImageName() {
        return props.getProperty("voltdb.image.name",
            "voltactivedata/volt-developer-edition");
    }

    public String getImageVersion() {
        return props.getProperty("voltdb.image.version", "14.1.0_voltdb");
    }

    public String getTestMode() {
        return props.getProperty("voltdb.test.mode", "testcontainer");
    }

    public boolean isTestContainerMode() {
        return "testcontainer".equalsIgnoreCase(getTestMode());
    }

    public boolean isShutdownOnCompletion() {
        return Boolean.parseBoolean(
            props.getProperty("voltdb.testcontainer.shutdown", "true"));
    }

    public String getExternalHost() {
        return props.getProperty("voltdb.external.host", "localhost");
    }

    public int getExternalPort() {
        return Integer.parseInt(
            props.getProperty("voltdb.external.port", "21211"));
    }

    public VoltDBCluster createTestContainer() {
        return new VoltDBCluster(
            getLicensePath(),
            getImageName() + ":" + getImageVersion(),
            getExtraLibDirectory()
        );
    }

    public void startAndConfigureTestContainer(VoltDBCluster db) {
        try {
            db.start();
            File jar = getProjectJar();
            if (jar != null) {
                System.out.println("Loading classes from: " + jar);
                ClientResponse response = db.loadClasses(jar.getAbsolutePath());
                assertEquals(ClientResponse.SUCCESS, response.getStatus(),
                    "Load classes must pass");
            }

            File schemaFile = extractResourceToTempFile("voltdb-ddl.sql");
            if (schemaFile != null) {
                System.out.println("Loading schema from classpath resource: ddl.sql");
                assertTrue(db.runDDL(schemaFile), "Schema must get loaded");
            } else {
                System.err.println("Schema resource not found: ddl.sql");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Client2 createExternalClient() throws Exception {
        Client2Config config = new Client2Config();
        Client2 client = ClientFactory.createClient(config);
        String host = getExternalHost();
        int port = getExternalPort();
        System.out.println("Connecting to external VoltDB at " + host + ":" + port);
        client.connectSync(host, port);
        return client;
    }

    public void configureExternalInstance(Client2 client) {
        try {
            new VoltDBSetup(client).initSchemaIfNeeded();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdownIfNeeded(VoltDBCluster db) {
        if (db != null && isShutdownOnCompletion()) {
            System.out.println("Shutting down VoltDB testcontainer...");
            db.shutdown();
        } else if (db != null) {
            System.out.println("Keeping VoltDB testcontainer running (shutdown disabled).");
        }
    }

    protected File extractResourceToTempFile(String resourcePath) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) return null;
            File tempFile = File.createTempFile("voltdb-", ".sql");
            tempFile.deleteOnExit();
            Files.copy(is, tempFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            return tempFile;
        } catch (IOException e) {
            throw new RuntimeException("Failed to extract resource: " + resourcePath, e);
        }
    }

    protected String getExtraLibDirectory() {
        File libdir = new File("target/lib");
        if (libdir.exists() && libdir.isDirectory() &&
            Arrays.stream(libdir.listFiles())
                .anyMatch(file -> file.getName().toLowerCase().endsWith(".jar"))) {
            return libdir.getAbsolutePath();
        }
        return null;
    }

    protected File getProjectJar() {
        String jarPath = props.getProperty("project.jar.path");
        if (jarPath != null) {
            File jar = new File(jarPath);
            if (jar.exists()) {
                return jar;
            }
        }
        return null;
    }

    protected String getLicensePath() {
        String envLicense = System.getenv("VOLTDB_LICENSE");
        if (envLicense != null) {
            File file = Paths.get(envLicense).toAbsolutePath().toFile();
            if (file.exists()) {
                System.out.println("License file path is: " + file.getAbsolutePath());
                return file.getAbsolutePath();
            }
        }
        String homeLicense = System.getProperty("user.home") + "/voltdb-license.xml";
        File homeFile = new File(homeLicense);
        if (homeFile.exists()) {
            System.out.println("License file path is: " + homeFile.getAbsolutePath());
            return homeFile.getAbsolutePath();
        }
        String defaultPath = "/tmp/voltdb-license.xml";
        System.out.println("License file path is: " + defaultPath);
        return defaultPath;
    }
}
