package com.gcp.lab1.example;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableRow;

/**
 * Class for executing a Apache Beam pipeline
 *
 */
public class App {
	public static void main(String[] args) {
		/*
		 * using pipeline option we can configure diffrent aspects of the pipline.
		 * 
		 */
		PipelineOptions po = PipelineOptionsFactory.create();

		Pipeline p = Pipeline.create(po);

		PCollection<String> pubSubMsg = p.apply(PubsubIO.readStrings().fromTopic("projects/nttdata-c4e-bde/topics/uc1-dlq-topic-0"));

		PCollection<TableRow> bgtableRow = pubSubMsg.apply(ParDo.of(new ConvertToString()));

		bgtableRow.apply(BigQueryIO.writeTableRows().to("nttdata-c4e-bde:uc1_2.account")
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

		p.run().waitUntilFinish();
	}

	public static class ConvertToString extends DoFn<String, TableRow> {

		@ProcessElement
		public void processing(ProcessContext pc) {
			TableRow trow = new TableRow().set("id", pc.element().toString())
					.set("name", pc.element().toString())
					.set("surname", pc.element().toString());

			pc.output(trow);

		}
	}

}