import io.github.cdimascio.dotenv.Dotenv;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

class Config {
    public final String JAVA_ENV;
    public final String KAFKA_PASSWORD;
    public final boolean SHOULD_SKIP_AUTHENTICATION;
    public final boolean SHOULD_DEDUP_BY_KEY;
    public final String STATSD_API_KEY;
    public final String STATSD_ROOT;
    public final String STATSD_HOST;
    public final String KAFKA_BROKER;
    public final String TOPIC;
    public final String GROUP_ID;
    public final String TARGET_ENDPOINT;
    public final int TARGET_RETRY_COUNT;
    public final String DEAD_LETTER_TOPIC;
    public final int CONCURRENCY;
    public final String TRUSTSTORE_LOCATION;
    public final String KEYSTORE_LOCATION;


    Config() throws Exception {
        Dotenv dotenv = Dotenv.load();

        JAVA_ENV = getString(dotenv, "JAVA_ENV");
        SHOULD_SKIP_AUTHENTICATION = Boolean.parseBoolean(dotenv.get("SHOULD_SKIP_AUTHENTICATION"));
        SHOULD_DEDUP_BY_KEY = Boolean.parseBoolean(dotenv.get("SHOULD_DEDUP_BY_KEY"));
        STATSD_ROOT = getString(dotenv, "STATSD_ROOT");
        STATSD_HOST = getString(dotenv, "STATSD_HOST");
        KAFKA_BROKER = getString(dotenv, "KAFKA_BROKER");
        TOPIC = getString(dotenv, "TOPIC");
        GROUP_ID = getString(dotenv, "GROUP_ID");
        TARGET_ENDPOINT = getString(dotenv, "TARGET_ENDPOINT");
        TARGET_RETRY_COUNT = parseInt(dotenv.get("TARGET_RETRY_COUNT"), 1);
        String dead_letter_topic = dotenv.get("DEAD_LETTER_TOPIC");
        DEAD_LETTER_TOPIC = dead_letter_topic == null ? TOPIC + "-dead-letter" : dead_letter_topic;
        CONCURRENCY = parseInt(dotenv.get("CONCURRENCY"), 1);

        JSONObject secrets = readSecrets(getString(dotenv, "SECRETS_FILE_LOCATION"));

        KAFKA_PASSWORD = getSecret(secrets, dotenv, "KAFKA_PASSWORD");
        STATSD_API_KEY = getSecret(secrets, dotenv, "STATSD_API_KEY");
        String truststore = getSecret(secrets, dotenv, "TRUSTSTORE");
        String keystore = getSecret(secrets, dotenv, "KEYSTORE");
        TRUSTSTORE_LOCATION = "client.truststore.jks";
        KEYSTORE_LOCATION = "client.keystore.p12";

        writeToFile(TRUSTSTORE_LOCATION, truststore);
        writeToFile(KEYSTORE_LOCATION, keystore);
    }

    private void writeToFile(String path, String value) throws IOException {

        Files.write(Paths.get(path), Base64.getDecoder().decode(value.getBytes(StandardCharsets.UTF_8)));
    }


    private JSONObject readSecrets(String secretsFileLocation) {
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
        } catch (JSONException ignored) {}

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

    private static int parseInt(String text, int fallback) {
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
