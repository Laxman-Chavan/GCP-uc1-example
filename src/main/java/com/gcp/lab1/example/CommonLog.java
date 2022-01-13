package com.gcp.lab1.example;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/**
 * A class used for parsing JSON web server events
 * Annotated with @DefaultSchema to the allow the use of Beam Schema and <Row> object
 */
@DefaultSchema(JavaFieldSchema.class)
public class CommonLog {
    String user_id;
    String ip;
    @javax.annotation.Nullable Double lat;
    @javax.annotation.Nullable Double lng;
    String timestamp;
    String http_request;
    String user_agent;
    Long http_response;
    Long num_bytes;
}