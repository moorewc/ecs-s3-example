package com.dell.jarvis;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.jersey.S3JerseyClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ECSClient {
    private static final String ECS_ACCESS_KEY_ID = System.getenv("ECS_ACCESS_KEY_ID");
    private static final String ECS_SECRET_ACCESS_KEY = System.getenv("ECS_SECRET_ACCESS_KEY");

    private static String ECS_BUCKET_NAME = null;
    private static String ECS_NAMESPACE = null;
    private static String FILE_PATH = null;
    private static Long ECS_RETENTION = null;
    private static String ECS_URI = null;

    // Parse command line
    private static void parseCommandLine(String[] args) {
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        options.addRequiredOption("b", "bucket", true, "A unique bucket name to store objects")
            .addRequiredOption("u", "uri", true, "The end point of the ECS S3 REST interface")
            .addRequiredOption("f", "file", true, "The path to the file to be uploaded to the ECS bucket")
            .addOption("n", "namespace", true, "The optional namespace within ECS - leave blank to use the default namespace")
            .addOption("r", "retention", true, "Retention in seconds.");


        try {
            cmd = parser.parse(options,args);
            
            ECS_BUCKET_NAME = cmd.getOptionValue("b");
            ECS_URI = cmd.getOptionValue("u");
            FILE_PATH = cmd.getOptionValue("f");

            if(cmd.hasOption("n")) {
                ECS_NAMESPACE = cmd.getOptionValue("n");
            }

            if (cmd.hasOption("r")) {
                ECS_RETENTION = Long.parseLong(cmd.getOptionValue("r"));
            }

        } catch (ParseException pe) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "Log messages to sequence diagrams converter", options );
            System.exit(1); 
        }

        return;
    }

    // Create an S3Client
    private static S3Client getS3Client() throws URISyntaxException {
        S3Config config = new S3Config(new URI(ECS_URI));

        config.withIdentity(ECS_ACCESS_KEY_ID).withSecretKey(ECS_SECRET_ACCESS_KEY);

        S3Client client = new S3JerseyClient(config);

        return client;
    }

    private static FileInputStream getFileInputStream() {
        FileInputStream inputStream = null;
        try {
            File file = new File(FILE_PATH);
            inputStream = new FileInputStream(file);


        } catch(FileNotFoundException e) {
            System.out.println("File not found" + e);
            System.exit(99);
        }

        return inputStream;
    }

    public static void main(String[] args) throws Exception {
        // Parse command line
        parseCommandLine(args);

        // Exit if ECS_ACCESS_KEY_ID or ECS_SECRET_ACCESS_KEY are not set
        if (ECS_ACCESS_KEY_ID == null || ECS_SECRET_ACCESS_KEY == null) {
            System.out.println("Unable to locate credentials\n");
            System.out.println("export ECS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE");
            System.out.println("export ECS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
            System.out.println("\n");
            System.exit(253);
        }

        // Create an S3 client
        S3Client client = getS3Client();

        // Read data from file which will be uploaded to S3 bucket
        FileInputStream inputStream = getFileInputStream();

        // Create meta data object which is used to set retention
        S3ObjectMetadata objectMetadata = new S3ObjectMetadata();

        // Add retention meta-data if retention is defined
        if (ECS_RETENTION != null) {
            objectMetadata.setRetentionPeriod(ECS_RETENTION);
        }
        
        // Create a put request with defined metadata.
        PutObjectRequest req = new PutObjectRequest(ECS_BUCKET_NAME, FILE_PATH, inputStream)
            .withObjectMetadata(objectMetadata);

        // upload object to S3 bucket
        client.putObject(req);
    }
}