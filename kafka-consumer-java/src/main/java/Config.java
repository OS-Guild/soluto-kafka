import io.github.cdimascio.dotenv.Dotenv;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

class Config {
    //Required
    public static String KAFKA_BROKER;
    public static String TOPIC;
    public static String GROUP_ID;
    public static String SENDING_PROTOCOL;
    public static String TARGET_GRPC_HOST;
    public static int TARGET_GRPC_PORT;
    public static String TARGET_HTTP_HOST;
    public static int TARGET_HTTP_PORT;

    //Optional
    public static String RETRY_TOPIC;
    public static String DEAD_LETTER_TOPIC;
    public static boolean DEDUP_PARTITION_BY_KEY;
    public static int CONCURRENCY;
    public static int CONCURRENCY_PER_PARTITION;
    public static int PROCESSING_DELAY;
    public static int POLL_RECORDS;
    public static int CONSUMER_POLL_TIMEOUT;
    public static int CONSUMER_THREADS;
    public static int IS_ALIVE_PORT;

    //Monitoring
    public static boolean AUTHENTICATED_KAFKA;
    public static String KAFKA_PASSWORD;
    public static String TRUSTSTORE_LOCATION;
    public static String KEYSTORE_LOCATION;

    //Statsd
    public static boolean STATSD_MONITOR;
    public static String STATSD_CONSUMER_NAME;
    public static String STATSD_API_KEY;
    public static String STATSD_ROOT;
    public static String STATSD_HOST;

    public static void init() throws Exception {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

        KAFKA_BROKER = getString(dotenv, "KAFKA_BROKER");
        TOPIC = getString(dotenv, "TOPIC");
        GROUP_ID = getString(dotenv, "GROUP_ID");

        SENDING_PROTOCOL = getString(dotenv, "SENDING_PROTOCOL");
        if (SENDING_PROTOCOL.equals("http")) {
            TARGET_HTTP_HOST = getString(dotenv, "TARGET_HTTP_HOST");
            TARGET_HTTP_PORT = getInt(dotenv, "TARGET_HTTP_PORT");
        }
        else if (SENDING_PROTOCOL.equals("grpc")) {
            TARGET_GRPC_HOST = getString(dotenv, "TARGET_GRPC_HOST");
            TARGET_GRPC_PORT = getInt(dotenv, "TARGET_GRPC_PORT");
        }
        else {
            throw new Exception("invalid SENDING_PROTOCOL, should be either http or grpc");
        }    
        
        RETRY_TOPIC = getOptionalString(dotenv, "RETRY_TOPIC", null);
        DEAD_LETTER_TOPIC = getOptionalString(dotenv, "DEAD_LETTER_TOPIC", null);
        CONCURRENCY = getOptionalInt(dotenv, "CONCURRENCY", 1);
        CONCURRENCY_PER_PARTITION = getOptionalInt(dotenv, "CONCURRENCY_PER_PARTITION", 1);
        DEDUP_PARTITION_BY_KEY = getOptionalBool(dotenv, "DEDUP_PARTITION_BY_KEY", false);
        PROCESSING_DELAY = getOptionalInt(dotenv, "PROCESSING_DELAY", 0);
        CONSUMER_POLL_TIMEOUT = getOptionalInt(dotenv, "CONSUMER_POLL_TIMEOUT", 100);
        CONSUMER_THREADS = getOptionalInt(dotenv, "CONSUMER_THREADS", 4);
        POLL_RECORDS = getOptionalInt(dotenv, "POLL_RECORDS", 50);
        IS_ALIVE_PORT = getOptionalInt(dotenv, "IS_ALIVE_PORT", 0);
        
        AUTHENTICATED_KAFKA = getBool(dotenv, "AUTHENTICATED_KAFKA");
        if (AUTHENTICATED_KAFKA) {
            JSONObject secrets = readSecrets(getString(dotenv, "SECRETS_FILE_LOCATION"));
            KAFKA_PASSWORD = getSecret(secrets, dotenv, "KAFKA_PASSWORD");
            String truststore = getSecret(secrets, dotenv, "TRUSTSTORE");
            String keystore = getSecret(secrets, dotenv, "KEYSTORE");
            TRUSTSTORE_LOCATION = "client.truststore.jks";
            KEYSTORE_LOCATION = "client.keystore.p12";
            writeToFile(TRUSTSTORE_LOCATION, truststore);
            writeToFile(KEYSTORE_LOCATION, keystore);
        }

        STATSD_MONITOR = getOptionalBool(dotenv, "STATSD_MONITOR", false);
        if (STATSD_MONITOR) {
            JSONObject secrets = readSecrets(getString(dotenv, "SECRETS_FILE_LOCATION"));
            STATSD_CONSUMER_NAME = getString(dotenv, "STATSD_CONSUMER_NAME");
            STATSD_API_KEY = getSecret(secrets, dotenv, "STATSD_API_KEY");
            STATSD_ROOT = getString(dotenv, "STATSD_ROOT");
            STATSD_HOST = getString(dotenv, "STATSD_HOST");
        }
    }

    private static void writeToFile(String path, String value) throws IOException {
        Files.write(Paths.get(path), Base64.getDecoder().decode(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static JSONObject readSecrets(String secretsFileLocation) {
        try {
            return new JSONObject(new String(Files.readAllBytes(Paths.get(secretsFileLocation))));
        } catch (IOException e) {
            return new JSONObject();
        }
    }

    private static String getSecret(JSONObject secrets, Dotenv dotenv, String name) throws Exception {
        String value = dotenv.get(name);

        if (value != null) {
            return value;
        }

        String secret = null;

        try {
            secret = secrets.getString(name);
        } catch (JSONException ignored) {
        }

        if (secret == null) {
            throw new Exception("missing secret: " + name);
        }

        return secret;
    }

    private static String getString(Dotenv dotenv, String name) throws Exception {
        String value = dotenv.get(name);

        if (value == null) {
            throw new Exception("missing env var: " + name);
        }

        return value;
    }

    private static String getOptionalString(Dotenv dotenv, String name, String fallback) {
        try {
            return getString(dotenv, name);
        } catch (Exception e) {
            return fallback;
        }
    }    

    private static int getInt(Dotenv dotenv, String name) {
        return Integer.parseInt(dotenv.get(name));
    }

    private static boolean getBool(Dotenv dotenv, String name) {
        return Boolean.parseBoolean(dotenv.get(name));
    }    

    private static boolean getOptionalBool(Dotenv dotenv, String name, boolean fallback) {
        try {
            return Boolean.parseBoolean(getString(dotenv, name));
        } catch (Exception e) {
            return fallback;
        }
    }

    private static int getOptionalInt(Dotenv dotenv, String name, int fallback) {
        try {
            return Integer.parseInt(dotenv.get(name));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
