package pubsub_to_bigquery;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.transforms.windowing.*;

import org.apache.beam.sdk.values.PCollection;

import org.apache.beam.sdk.values.PCollectionTuple;

import org.joda.time.Duration;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_NEVER;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static pubsub_to_bigquery.TransformToBQ.FAILURE_TAG;
import static pubsub_to_bigquery.TransformToBQ.SUCCESS_TAG;

public class App {

	public interface MyOptions extends DataflowPipelineOptions, PipelineOptions {
    @Description("BigQuery project")
    String getBQProject();

    void setBQProject(String value);

    @Description("BigQuery dataset")
    String getBQDataset();

    void setBQDataset(String value);

    @Description("Bucket path to collect pipeline errors in json files")

    String getErrorsBucket();
    void setErrorsBucket(String value);

    @Description("Dead letter queue path")
    String getDLQ();
    void setDLQ(String value);

    @Description("Bucket path to collect pipeline errors in json files")
    String getBucket();

    void setBucket(String value);

    @Description("Pubsub project")
    String getPubSubProject();

    void setPubSubProject(String value);

    @Description("Pubsub subscription")
    String getSubscription();

    void setSubscription(String value);
  }

    public static void main(String[] args) {

        PipelineOptionsFactory.register(MyOptions.class);
  		MyOptions options = PipelineOptionsFactory.fromArgs(args)
                                                  .withValidation()
                                                  .as(MyOptions.class);
        Pipeline p = Pipeline.create(options);
        final String SUBSCRIPTION = String.format("projects/%s/subscriptions/%s", options.getPubSubProject(), options.getSubscription());

        final int STORAGE_LOAD_INTERVAL = 1; // minutes

        final String ERROR_QUEUE = String.format("projects/%s/topics/uc1-dlq-topic-8", options.getPubSubProject());
        final String BQ_PROJECT = options.getBQProject();
        final String BQ_DATASET = options.getBQDataset();

        System.out.println(options);

        // 1. Read from PubSub
        PCollection<String> pubsubMessages = p
                .apply("ReadPubSubSubscription", PubsubIO.readStrings().fromSubscription(SUBSCRIPTION));



        // 2. Transform element to TableRow
        PCollectionTuple results = pubsubMessages.apply("TransformToBQ", TransformToBQ.run());

        // 3. Write the successful records out to BigQuery

         results.get(SUCCESS_TAG).apply("WriteSuccessfulRecordsToBQ", BigQueryIO.writeTableRows()
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()) //Retry all failures except for known persistent errors.
                .withWriteDisposition(WRITE_APPEND)
                .withCreateDisposition(CREATE_NEVER)
                .to((row) -> {
                    String tableName = "account";
                    return new TableDestination(String.format("%s:%s.%s", BQ_PROJECT, BQ_DATASET, tableName), "Some destination");
                })
        );


        // 4. write all 'bad' data to ERRORS_QUEUE with STORAGE_LOAD_INTERVAL
         results.get(FAILURE_TAG)
                .apply("Window Errors", Window.<String>into(new GlobalWindows())
                .triggering(Repeatedly
                        .forever(AfterProcessingTime
                                .pastFirstElementInPane()
                                .plusDelayOf(Duration.standardMinutes(STORAGE_LOAD_INTERVAL)))
                )
                .withAllowedLateness(Duration.standardMinutes(1))
                .discardingFiredPanes()
        )
                .apply("Write to PubSub",PubsubIO.writeStrings().to(ERROR_QUEUE));
        p.run();

    }
}
