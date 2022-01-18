package com.gcp.lab1.example;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Class for executing a Apache Beam pipeline
 *
 */
public class App {
	/**
	 * The logger to output status messages to.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(App.class);
	// declared input paramters
	private static final String JOB_NAME = "usecase1-labid-2";
	private static final String REGION = "europe-west4";
	private static final String GCP_TEMP_LOCATION = "gs://c4e-uc1-dataflow-temp-2/temp";
	private static final String STAGING_LOCATION = "gs://c4e-uc1-dataflow-temp-2/staging";
	private static final String SERVICE_ACCOUNT = "c4e-uc1-sa-2@nttdata-c4e-bde.iam.gserviceaccount.com";
	private static final String WORKER_MACHINE_TYPE = "n1-standard-1";
	private static final String SUBNETWORK = "regions/europe-west4/subnetworks/subnet-uc1-2";
	// declared bq table and pub-sub topic
	private static final String BQ_TABLE = "nttdata-c4e-bde:uc1_2.account";
	private static final String TOPIC_SUB = "projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-2";
	private static final String DLQ_TOPIC = "projects/nttdata-c4e-bde/topics/uc1-dlq-topic-2";

	public static final TupleTag<CommonLog> VALID_MSG = new TupleTag<CommonLog>() {
		private static final long serialVersionUID = 1L;
	};
	public static final TupleTag<String> INVALID_MSG = new TupleTag<String>() {
		private static final long serialVersionUID = 1L;
	};

	public static final Schema BQ_SCHEMA = Schema.builder().addInt64Field("id").addStringField("name")
			.addStringField("surname").build();

	/**
	 * The main entry-point for pipeline execution. This method will start the
	 * pipeline but will not wait for it's execution to finish. If blocking
	 * execution is required, use the
	 * 
	 * @param args The command-line args passed by the executor.
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
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

		run(options);

	}

	public static class JsonToCommonLog extends DoFn<String, CommonLog> {

		private static final long serialVersionUID = 1L;

		public static PCollectionTuple convert(PCollection<String> input) throws Exception {
			return input.apply("JsonToCommonLog", ParDo.of(new DoFn<String, CommonLog>() {

				private static final long serialVersionUID = 1L;

				@ProcessElement
				public void processElement(@Element String record, ProcessContext context) {

					try {
						Gson gson = new Gson();
						CommonLog commonLog = gson.fromJson(record, CommonLog.class);
						context.output(VALID_MSG, commonLog);
					} catch (Exception e) {
						e.printStackTrace();
						context.output(INVALID_MSG, record);
					}
				}
			}).withOutputTags(VALID_MSG, TupleTagList.of(INVALID_MSG)));
		}
	}

	// method to execute the apache beam pipeline
	public static void run(DataflowPipelineOptions options) throws Exception {
		Pipeline p = Pipeline.create(options);

		PCollection<String> pubSubMessages = p.apply("ReadPubSubMessage",
				PubsubIO.readStrings().fromSubscription(TOPIC_SUB));

		PCollectionTuple pubSubMessagesdata = JsonToCommonLog.convert(pubSubMessages);

		pubSubMessagesdata.get(VALID_MSG).apply("ConvertCommonLogToJson", ParDo.of(new DoFn<CommonLog, String>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext context) {
				Gson gsonObj = new Gson();
				String jsonRecord = gsonObj.toJson(context.element());
				context.output(jsonRecord);
			}
		}))

				.apply("ConvertJsontoRow", JsonToRow.withSchema(BQ_SCHEMA))

				.apply("InserttoBQTable",
						BigQueryIO.<Row>write().to(BQ_TABLE).useBeamSchema()
								.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
								.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		/**
		 * Writing INVALID Messages into the DEAD LETTER TOPIC
		 */
		pubSubMessagesdata.get(INVALID_MSG).apply("WriteToDLQTopic", PubsubIO.writeStrings().to(DLQ_TOPIC));
		LOG.info("Building pipeline......................");
		p.run().waitUntilFinish();
	}
}