package com.gcp.lab1.example;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.runners.dataflow.DataflowRunner;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;

/**
 * Class for executing a Apache Beam pipeline
 *
 */
public class App {
	/**
	 * The logger to output status messages to.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(App.class);
	
	private static final String JOB_NAME="usecase1-labid-2";
	private static final String REGION="europe-west4";
	private static final String GCP_TEMP_LOCATION="gs://c4e-uc1-dataflow-temp-2/temp";
	private static final String STAGING_LOCATION="gs://c4e-uc1-dataflow-temp-2/staging";
	private static final String SERVICE_ACCOUNT="c4e-uc1-sa-2@nttdata-c4e-bde.iam.gserviceaccount.com";
	private static final String WORKER_MACHINE_TYPE="n1-standard-1";
	private static final String SUBNETWORK="regions/europe-west4/subnetworks/subnet-uc1-2";
	
	private static final String BQ_TABLE="nttdata-c4e-bde:uc1_2.account";
	private static final String TOPIC_SUB="projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-2";

	
  
	/**
	 * The main entry-point for pipeline execution. This method will start the
	 * pipeline but will not wait for it's execution to finish. If blocking
	 * execution is required, use the
	 * 
	 * @param args The command-line args passed by the executor.
	 */
	public static void main(String[] args) {
		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(DataflowPipelineOptions.class);
		// set the command line attributes
		options.setJobName(JOB_NAME);
		options.setRegion(REGION);
		options.setRunner(DataflowRunner.class);
		options.setGcpTempLocation(GCP_TEMP_LOCATION);
		options.setStagingLocation(STAGING_LOCATION);
		options.setServiceAccount(SERVICE_ACCOUNT);
		options.setWorkerMachineType(WORKER_MACHINE_TYPE);
		options.setSubnetwork(SUBNETWORK);
		options.setStreaming(true);

		runPipeline(options);

	}

	// method to execute the apache beam pipeline
	public static void runPipeline(DataflowPipelineOptions options) {
		// Build the table schema for the output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("name").setType("STRING"));
		fields.add(new TableFieldSchema().setName("surname").setType("STRING"));
		fields.add(new TableFieldSchema().setName("id").setType("INTEGER"));
		TableSchema schema = new TableSchema().setFields(fields);
		// created pipeline object
		Pipeline p = Pipeline.create(options);
		p.apply("ReadPubSubMessage",
				PubsubIO.readMessagesWithAttributes()
						.fromSubscription(TOPIC_SUB))
				.apply("WindowByMinute", Window.into(FixedWindows.of(Duration.standardSeconds(60))))

				.apply("ConvertDataToTableRows", ParDo.of(new DoFn<PubsubMessage, TableRow>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						PubsubMessage message = c.element();
						int id = 0;
						try {
							id = Integer.parseInt(message.getAttribute("id"));
						} catch (NumberFormatException e) {
							e.getMessage();
						}
						// get the attributes from pub sub message
						String name = message.getAttribute("name");
						String surname = message.getAttribute("surname");
						// set the attributes in the table row
						TableRow row = new TableRow().set("id", id).set("name", name).set("surname", surname);
						c.output(row);
					}
				})).apply("InsertTableRowsToBigQuery",
						BigQueryIO.writeTableRows().to(BQ_TABLE).withSchema(schema)
								.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
								.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

		LOG.info("Building pipeline......................");
		p.run().waitUntilFinish();
	}

}