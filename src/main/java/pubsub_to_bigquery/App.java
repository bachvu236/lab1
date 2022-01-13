/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package pubsub_to_bigquery;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
//import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_NEVER;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
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

    @Description("Bucket path to collect pipeline errors in json files")
    String getBucket();

    void setBucket(String value);

    @Description("Pubsub project")
    String getPubSubProject();

    void setPubSubProject(String value);

    @Description("Pubsub subscription")
    String getInputTopic();

    void setInputTopic(String inputTopic);
  }

      /** The log to output status messages to. */
    private static Logger LOG = LoggerFactory.getLogger(App.class);

    /**
     * class {@link ErrorFormatFileName} implement file naming format for files in errors bucket (failed rows)
     * used in withNaming
     */
    private static class ErrorFormatFileName implements FileIO.Write.FileNaming {

        private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd");
        private static final DateTimeFormatter TIME_FORMAT = DateTimeFormat.forPattern("HH:mm:ss");

        private final String filePrefix;

        ErrorFormatFileName(String prefix) {
            filePrefix = prefix;
        }

        /**
         * Create filename in specific format
         *
         * @return A string representing filename.
         */
        @Override
        public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
            //IntervalWindow intervalWindow = (IntervalWindow) window;

            return String.format(
                    "%s/%s/error-%s-%s-of-%s%s",
                    filePrefix,
                    DATE_FORMAT.print(new DateTime()),
                    TIME_FORMAT.print(new DateTime()),
                    shardIndex,
                    numShards,
                    compression.getSuggestedSuffix());
        }
    }

    public static void main(String[] args) {

        PipelineOptionsFactory.register(MyOptions.class);
  		MyOptions options = PipelineOptionsFactory.fromArgs(args)
                                                  .withValidation()
                                                  .as(MyOptions.class);
        Pipeline p = Pipeline.create(options);
        options.setJobName("uc_1_8");
        final String TOPIC = String.format("projects/%s/topics/%s", options.getPubSubProject(), options.getInputTopic());

        final int STORAGE_LOAD_INTERVAL = 1; // minutes
        final int STORAGE_NUM_SHARDS = 1;

        final String BQ_PROJECT = options.getBQProject();
        final String BQ_DATASET = options.getBQDataset();

        System.out.println(options);

        // 1. Read from PubSub
        PCollection<String> pubsubMessages = p
                .apply("ReadPubSubSubscription", PubsubIO.readStrings().fromTopic(TOPIC));

        // 2. Count PubSub Data
        pubsubMessages.apply("CountPubSubData", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c)  {
                        Metric.pubsubMessages.inc();
                    }
                }));

        // 3. Transform element to TableRow
        PCollectionTuple results = pubsubMessages.apply("TransformToBQ", TransformToBQ.run());

        // 4. Write the successful records out to BigQuery

        WriteResult writeResult = results.get(SUCCESS_TAG).apply("WriteSuccessfulRecordsToBQ", BigQueryIO.writeTableRows()
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()) //Retry all failures except for known persistent errors.
                .withWriteDisposition(WRITE_APPEND)
                .withCreateDisposition(CREATE_NEVER)
                .withExtendedErrorInfo() //- getFailedInsertsWithErr
                .ignoreUnknownValues()
                .skipInvalidRows()
                .withoutValidation()
                .to((row) -> {
                    String tableName = Objects.requireNonNull(row.getValue()).get("event_type").toString();
                    return new TableDestination(String.format("%s:%s.%s", BQ_PROJECT, BQ_DATASET, tableName), "Some destination");
                })
        );
        
        // 5. Write rows that failed to GCS using windowing of STORAGE_LOAD_INTERVAL interval
        // Flatten failed rows after TransformToBQ with failed inserts

        PCollection<KV<String, String>> failedInserts = writeResult.getFailedInsertsWithErr()
                .apply("MapFailedInserts", MapElements.via(new SimpleFunction<BigQueryInsertError, KV<String, String>>() {
                                                               @Override
                                                               public KV<String, String> apply(BigQueryInsertError input) {
                                                                   return KV.of("FailedInserts", input.getError().toString() + " for table" + input.getRow().get("table") + ", message: "+ input.getRow().toString());
                                                               }
                                                           }
                ));

        // 6. Count failed inserts
        failedInserts.apply("LogFailedInserts", ParDo.of(new DoFn<KV<String, String>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c)  {
                LOG.error("{}: {}", c.element().getKey(), c.element().getValue());
                Metric.failedInsertMessages.inc();
            }
        }));


        // 7. write all 'bad' data to ERRORS_BUCKET with STORAGE_LOAD_INTERVAL
//        PCollectionList<KV<String, String>> allErrors = PCollectionList.of(results.get(FAILURE_TAG)).and(failedInserts);
//        allErrors.apply(Flatten.<KV<String, String>>pCollections())
//                .apply("Window Errors", Window.<KV<String, String>>into(new GlobalWindows())
//                .triggering(Repeatedly
//                        .forever(AfterProcessingTime
//                                .pastFirstElementInPane()
//                                .plusDelayOf(Duration.standardMinutes(STORAGE_LOAD_INTERVAL)))
//                )
//                .withAllowedLateness(Duration.standardDays(1))
//                .discardingFiredPanes()
//        )
//                .apply("WriteErrorsToGCS", FileIO.<String, KV<String, String>>writeDynamic()
//                        .withDestinationCoder(StringUtf8Coder.of())
//                        .by(KV::getKey)
//                        .via(Contextful.fn(KV::getValue), TextIO.sink())
//                        .withNumShards(STORAGE_NUM_SHARDS)
//                        .to(ERRORS_BUCKET)
//                        .withNaming(ErrorFormatFileName::new));


        p.run();


    }
}
