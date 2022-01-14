package pubsub_to_bigquery;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
@VisibleForTesting
/**
 * A class used for parsing JSON web server events
 */
@DefaultSchema(JavaFieldSchema.class)
public class Account {
    Integer id;
    String name;
    String surname;
    @javax.annotation.Nullable Double lat;
    @javax.annotation.Nullable Double lng;
    String timestamp;
    String http_request;
    String user_agent;
    Long http_response;
    Long num_bytes;
}
