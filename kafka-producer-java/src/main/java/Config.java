import io.github.cdimascio.dotenv.Dotenv;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

class Config {
    public static String PRODUCER_NAME;
    public static String JAVA_ENV;
    public static int PORT;
    public static String KAFKA_PASSWORD;
    public static boolean SHOULD_SKIP_AUTHENTICATION;
    public static String STATSD_API_KEY;
    public static String STATSD_ROOT;
    public static String STATSD_HOST;
    public static String KAFKA_BROKER;
    public static String TOPIC;
    public static String READINESS_TOPIC;
    public static String TRUSTSTORE_LOCATION;
    public static String KEYSTORE_LOCATION;
    public static int LINGER_TIME_MS;
    public static String COMPRESSION_TYPE;
    public static String CLUSTER;

    public static void init() throws Exception {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

        JAVA_ENV = getString(dotenv, "JAVA_ENV");
        PORT = getInt(dotenv, "PORT");
        SHOULD_SKIP_AUTHENTICATION = getOptionalBoolean(dotenv, "SHOULD_SKIP_AUTHENTICATION", false);
        STATSD_ROOT = getString(dotenv, "STATSD_ROOT");
        STATSD_HOST = getString(dotenv, "STATSD_HOST");
        KAFKA_BROKER = getString(dotenv, "KAFKA_BROKER");
        TOPIC = getString(dotenv, "TOPIC");
        READINESS_TOPIC = getOptionalString(dotenv, "READINESS_TOPIC", null);
        PRODUCER_NAME = getString(dotenv, "PRODUCER_NAME");
        LINGER_TIME_MS = getOptionalInt(dotenv, "LINGER_TIME_MS", 0);
        COMPRESSION_TYPE = getOptionalString(dotenv, "COMPRESSION_TYPE", "none");
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

    private static String getOptionalString(Dotenv dotenv, String name, String defaultString) throws Exception {
        String value = dotenv.get(name);

        if (value == null) {
            return defaultString;
        }

        return value;
    }

    private static boolean getOptionalBoolean(Dotenv dotenv, String name, boolean fallback) {
        try {
            return Boolean.parseBoolean(getString(dotenv, name));
        } catch (Exception e) {
            return fallback;
        }
    }

    private static int getInt(Dotenv dotenv, String name) {
        return Integer.parseInt(dotenv.get(name));
    }

    private static int getOptionalInt(Dotenv dotenv, String name, int fallback) {
        try {
            return Integer.parseInt(dotenv.get(name));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
