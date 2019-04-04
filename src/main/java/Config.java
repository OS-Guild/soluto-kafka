import io.github.cdimascio.dotenv.Dotenv;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

class Config {
    public static String JAVA_ENV;
    public static int PORT;    
    public static String KAFKA_PASSWORD;
    public static boolean SHOULD_SKIP_AUTHENTICATION;
    public static boolean SHOULD_DEDUP_BY_KEY;
    public static String STATSD_API_KEY;
    public static String STATSD_ROOT;
    public static String STATSD_HOST;
    public static String KAFKA_BROKER;
    public static String TOPIC;
    public static String RETRY_TOPIC;
    public static String POISON_MESSAGE_TOPIC;
    public static String GROUP_ID;
    public static String TARGET_ENDPOINT;
    public static int CONCURRENCY;
    public static int POLL_RECORDS;
    public static String TRUSTSTORE_LOCATION;
    public static String KEYSTORE_LOCATION;
    public static int CONSUMER_POLL_TIMEOUT;
	public static int CONSUMER_THREADS;
	public static String CLUSTER;

    public static void init() throws Exception {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

        JAVA_ENV = getString(dotenv, "JAVA_ENV");
        PORT = getInt(dotenv, "PORT");
        SHOULD_SKIP_AUTHENTICATION = getOptionalBoolean(dotenv, "SHOULD_SKIP_AUTHENTICATION", false);
        SHOULD_DEDUP_BY_KEY = getOptionalBoolean(dotenv, "SHOULD_DEDUP_BY_KEY", false);
        STATSD_ROOT = getString(dotenv, "STATSD_ROOT");
        STATSD_HOST = getString(dotenv, "STATSD_HOST");
        KAFKA_BROKER = getString(dotenv, "KAFKA_BROKER");
        TOPIC = getString(dotenv, "TOPIC");
        GROUP_ID = getString(dotenv, "GROUP_ID");
        TARGET_ENDPOINT = getString(dotenv, "TARGET_ENDPOINT");
        RETRY_TOPIC = getString(dotenv, "RETRY_TOPIC");
        POISON_MESSAGE_TOPIC = getString(dotenv, "POISON_MESSAGE_TOPIC");
        CONCURRENCY = getOptionalInt(dotenv, "CONCURRENCY", 1);
        CONSUMER_POLL_TIMEOUT = getOptionalInt(dotenv, "CONSUMER_POLL_TIMEOUT", 100);
        CONSUMER_THREADS = getOptionalInt(dotenv, "CONSUMER_THREADS", 4);
        POLL_RECORDS = getOptionalInt(dotenv, "POLL_RECORDS", 50);
        CLUSTER = getOptionalString(dotenv, "CLUSTER", "local");

        JSONObject secrets = readSecrets(getString(dotenv, "SECRETS_FILE_LOCATION"));
        KAFKA_PASSWORD = getSecret(secrets, dotenv, "KAFKA_PASSWORD");
        STATSD_API_KEY = getSecret(secrets, dotenv, "STATSD_API_KEY");
        TRUSTSTORE_LOCATION = "client.truststore.jks";
        KEYSTORE_LOCATION = "client.keystore.p12";

        String truststore = getSecret(secrets, dotenv, "TRUSTSTORE");
        String keystore = getSecret(secrets, dotenv, "KEYSTORE");
        writeToFile(TRUSTSTORE_LOCATION, truststore);
        writeToFile(KEYSTORE_LOCATION, keystore);
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

    private static int getInt(Dotenv dotenv, String name) {
        return Integer.parseInt(dotenv.get(name));
    }

    private static String getOptionalString(Dotenv dotenv, String name, String fallback) {
        try {
            return getString(dotenv, name);
        } catch (Exception e) {
            return fallback;
        }
    }

    private static boolean getOptionalBoolean(Dotenv dotenv, String name, boolean fallback) {
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
