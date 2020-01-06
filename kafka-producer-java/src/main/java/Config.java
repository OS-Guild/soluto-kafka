import io.github.cdimascio.dotenv.Dotenv;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import org.json.JSONException;
import org.json.JSONObject;

class Config {
    //Required
    public static int PORT;
    public static String KAFKA_BROKER;
    public static String TOPIC;

    //Optional
    public static String READINESS_TOPIC;
    public static int LINGER_TIME_MS;
    public static String COMPRESSION_TYPE;

    //Autentication
    public static boolean AUTHENTICATED_KAFKA;
    public static String TRUSTSTORE_LOCATION;
    public static String KEYSTORE_LOCATION;
    public static String KAFKA_PASSWORD;

    //Statsd monitoring
    public static boolean STATSD_CONFIGURED;
    public static String STATSD_PRODUCER_NAME;
    public static String STATSD_API_KEY;
    public static String STATSD_ROOT;
    public static String STATSD_HOST;

    public static void init() throws Exception {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

        PORT = getInt(dotenv, "PORT");
        KAFKA_BROKER = getString(dotenv, "KAFKA_BROKER");
        TOPIC = getString(dotenv, "TOPIC");

        READINESS_TOPIC = getOptionalString(dotenv, "READINESS_TOPIC", null);
        LINGER_TIME_MS = getOptionalInt(dotenv, "LINGER_TIME_MS", 0);
        COMPRESSION_TYPE = getOptionalString(dotenv, "COMPRESSION_TYPE", "none");

        JSONObject secrets = buildSecrets(dotenv);

        KAFKA_PASSWORD = getOptionalSecret(secrets, dotenv, "KAFKA_PASSWORD");
        String truststore = getOptionalSecret(secrets, dotenv, "TRUSTSTORE");
        String keystore = getOptionalSecret(secrets, dotenv, "KEYSTORE");

        AUTHENTICATED_KAFKA =
            validateAllParameterConfigured(
                "Missing kafka authentication variable",
                KAFKA_PASSWORD,
                truststore,
                keystore
            );
        if (AUTHENTICATED_KAFKA) {
            TRUSTSTORE_LOCATION = "client.truststore.jks";
            KEYSTORE_LOCATION = "client.keystore.p12";
            writeToFile(TRUSTSTORE_LOCATION, truststore);
            writeToFile(KEYSTORE_LOCATION, keystore);
        }

        STATSD_PRODUCER_NAME = getOptionalString(dotenv, "STATSD_PRODUCER_NAME", null);
        STATSD_API_KEY = getOptionalSecret(secrets, dotenv, "STATSD_API_KEY");
        STATSD_ROOT = getOptionalString(dotenv, "STATSD_ROOT", null);
        STATSD_HOST = getOptionalString(dotenv, "STATSD_HOST", null);
        STATSD_CONFIGURED =
            validateAllParameterConfigured(
                "Missing statsd variable",
                STATSD_PRODUCER_NAME,
                STATSD_API_KEY,
                STATSD_ROOT,
                STATSD_HOST
            );
    }

    private static boolean validateAllParameterConfigured(String error, String... values) throws Exception {
        if (Arrays.stream(values).allMatch(Objects::isNull)) {
            return false;
        }

        if (Arrays.stream(values).anyMatch(Objects::isNull)) {
            throw new Exception(error);
        }

        return true;
    }

    private static void writeToFile(String path, String value) throws IOException {
        Files.write(Paths.get(path), Base64.getDecoder().decode(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static JSONObject buildSecrets(Dotenv dotenv) {
        return readSecrets(getOptionalString(dotenv, "SECRETS_FILE_LOCATION", null));
    }

    private static JSONObject readSecrets(String secretsFileLocation) {
        if (secretsFileLocation == null) {
            return new JSONObject();
        }

        try {
            return new JSONObject(new String(Files.readAllBytes(Paths.get(secretsFileLocation))));
        } catch (IOException e) {
            return new JSONObject();
        }
    }

    private static String getOptionalSecret(JSONObject secrets, Dotenv dotenv, String name) {
        String value = dotenv.get(name);

        if (value != null) {
            return value;
        }

        String secret = null;

        try {
            secret = secrets.getString(name);
        } catch (JSONException ignored) {}

        return secret;
    }

    private static String getString(Dotenv dotenv, String name) throws Exception {
        String value = dotenv.get(name);

        if (value == null) {
            throw new Exception("missing env var: " + name);
        }

        return value;
    }

    private static String getOptionalString(Dotenv dotenv, String name, String defaultString) {
        String value = dotenv.get(name);

        if (value == null) {
            return defaultString;
        }

        return value;
    }

    private static boolean getOptionalBool(Dotenv dotenv, String name, boolean fallback) {
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
