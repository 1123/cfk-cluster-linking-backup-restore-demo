package demo;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Instant;
import java.util.Properties;

/** Shared configuration (OAuth over SASL_SSL against Keycloak) and evidence helpers. */
final class Common {
    static final Path EVIDENCE = Path.of(env("EVIDENCE_DIR", "/evidence"));

    private Common() {}

    static String env(String name, String def) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? def : v;
    }

    static String env(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) throw new IllegalStateException("missing env " + name);
        return v;
    }

    /**
     * The OAuth token endpoint is HTTPS with a certificate from the demo CA. The JVM default truststore is used
     * for that call, so a PKCS12 truststore is built from the mounted CA PEM before any Kafka class is loaded.
     */
    static void initJvmTrust() {
        String caPem = env("CA_PEM", "/etc/demo-ca/ca.pem");
        try (InputStream in = Files.newInputStream(Path.of(caPem))) {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(null, null);
            int i = 0;
            for (Certificate c : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
                ks.setCertificateEntry("demo-ca-" + i++, c);
            }
            Path p = Files.createTempFile("truststore", ".p12");
            char[] pw = "changeit".toCharArray();
            try (var out = Files.newOutputStream(p)) {
                ks.store(out, pw);
            }
            System.setProperty("javax.net.ssl.trustStore", p.toString());
            System.setProperty("javax.net.ssl.trustStorePassword", "changeit");
            System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");
        } catch (Exception e) {
            throw new IllegalStateException("cannot build JVM truststore from " + caPem, e);
        }
        System.setProperty("org.apache.kafka.sasl.oauthbearer.allowed.urls", env("KC_TOKEN_URL"));
    }

    /** Base client config: SASL_SSL + OAUTHBEARER (client credentials) + PEM truststore for the brokers. */
    static Properties baseConfig(String bootstrap, String clientIdEnv, String clientSecretEnv) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("security.protocol", "SASL_SSL");
        p.put("sasl.mechanism", "OAUTHBEARER");
        p.put("sasl.login.callback.handler.class",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler");
        p.put("sasl.oauthbearer.token.endpoint.url", env("KC_TOKEN_URL"));
        p.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required"
                + " clientId=\"" + env(clientIdEnv) + "\" clientSecret=\"" + env(clientSecretEnv) + "\";");
        p.put("ssl.truststore.type", "PEM");
        p.put("ssl.truststore.location", env("CA_PEM", "/etc/demo-ca/ca.pem"));
        // Re-resolve the bootstrap service if all known brokers are gone (primary deleted and recreated).
        p.put("metadata.recovery.strategy", "rebootstrap");
        p.put("metadata.recovery.rebootstrap.trigger.ms", "30000");
        p.put("reconnect.backoff.max.ms", "5000");
        return p;
    }

    /** Append-only line writer, flushed after every line so a killed pod loses nothing already logged. */
    static final class Evidence implements AutoCloseable {
        private final BufferedWriter w;

        Evidence(String fileName) {
            try {
                Files.createDirectories(EVIDENCE);
                w = Files.newBufferedWriter(EVIDENCE.resolve(fileName), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        synchronized void line(String s) {
            try {
                w.write(s);
                w.newLine();
                w.flush();
            } catch (IOException e) {
                System.err.println("evidence write failed: " + e);
            }
        }

        @Override
        public synchronized void close() throws IOException {
            w.close();
        }
    }

    static void writeAtomically(Path target, String content) throws IOException {
        Files.createDirectories(target.getParent());
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        Files.writeString(tmp, content, StandardCharsets.UTF_8);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    static String now() {
        return Instant.now().toString();
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
