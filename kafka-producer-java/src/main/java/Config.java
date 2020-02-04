import io.github.cdimascio.dotenv.Dotenv;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
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
    public static boolean AUTHENTICATED_KAFKA = false;
    public static String SECURITY_PROTOCOL;
    public static String TRUSTSTORE_LOCATION;
    public static String KEYSTORE_LOCATION;
    public static String TRUSTSTORE_PASSWORD;
    public static String KEYSTORE_PASSWORD;
    public static String KEY_PASSWORD;
    public static String SASL_USERNAME;
    public static String SASL_PASSWORD;

    //Statsd monitoring
    public static boolean STATSD_CONFIGURED = false;
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

        TRUSTSTORE_LOCATION = "client.truststore.jks";
        KEYSTORE_LOCATION = "client.keystore.p12";

        String truststore = getOptionalSecret(secrets, dotenv, "TRUSTSTORE", null);
        if (truststore != null) {
            writeToFile(TRUSTSTORE_LOCATION, truststore);
            TRUSTSTORE_PASSWORD = getSecret(secrets, dotenv, "TRUSTSTORE_PASSWORD");
        }

        SECURITY_PROTOCOL = getOptionalString(dotenv, "SECURITY_PROTOCOL", "");

        if (SECURITY_PROTOCOL.equals("SSL")) {
            KEYSTORE_PASSWORD = getSecret(secrets, dotenv, "KEYSTORE_PASSWORD");
            KEY_PASSWORD = getSecret(secrets, dotenv, "KEY_PASSWORD");
            String keystore = getSecret(secrets, dotenv, "KEYSTORE");
            writeToFile(KEYSTORE_LOCATION, keystore);
            AUTHENTICATED_KAFKA = true;
        }

        if (SECURITY_PROTOCOL.equals("SASL_SSL")) {
            SASL_USERNAME = getString(dotenv, "SASL_USERNAME");
            SASL_PASSWORD = getSecret(secrets, dotenv, "SASL_PASSWORD");
            AUTHENTICATED_KAFKA = true;
        }

        STATSD_PRODUCER_NAME = getOptionalString(dotenv, "STATSD_PRODUCER_NAME", null);
        if (STATSD_PRODUCER_NAME != null) {
            STATSD_API_KEY = getSecret(secrets, dotenv, "STATSD_API_KEY");
            STATSD_ROOT = getString(dotenv, "STATSD_ROOT");
            STATSD_HOST = getString(dotenv, "STATSD_HOST");
            STATSD_CONFIGURED = true;
        }
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

    private static String getSecret(JSONObject secrets, Dotenv dotenv, String name) throws Exception {
        String value = dotenv.get(name);

        if (value != null) {
            return value;
        }

        String secret = null;

        try {
            secret = secrets.getString(name);
        } catch (JSONException ignored) {
            throw new Exception("missing env var: " + name);
        }

        return secret;
    }

    private static String getOptionalSecret(JSONObject secrets, Dotenv dotenv, String name, String fallback) {
        try {
            return getSecret(secrets, dotenv, name);
        } catch (Exception e) {
            return fallback;
        }
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
