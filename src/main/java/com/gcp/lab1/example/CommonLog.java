package com.gcp.lab1.example;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

/**
 * A class used for parsing JSON web server events Annotated with @DefaultSchema
 * to the allow the use of Beam Schema and <Row> object
 */
@DefaultSchema(JavaFieldSchema.class)
public class CommonLog {
	int id;
	@javax.annotation.Nullable
	String name;
	@javax.annotation.Nullable
	String surname;

	 @SchemaCreate
	    public CommonLog(int id,String name,String surname) {
	     this.id=id;
	     this.name=name;
	     this.surname=surname;
	    }
}