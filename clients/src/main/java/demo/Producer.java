package demo;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Sequenced producer that keeps running through the whole incident (D–F).
 * Every delivery outcome is logged to /evidence/producer-&lt;topic&gt;.log:
 * ts, topic, seq, partition, offset, status (ACKED | FAILED:&lt;exception&gt;), producer instance.
 * Callbacks only log and flag; the producer instance is replaced in the main thread.
 */
final class Producer {
    private final String topic = Common.env("TOPIC");
    private final String bootstrap = Common.env("BOOTSTRAP");
    private final int ratePerSec = Integer.parseInt(Common.env("RATE", "20"));
    private final String runId = UUID.randomUUID().toString();
    private final Common.Evidence log = new Common.Evidence("producer-" + topic + ".log");
    private final AtomicReference<String> replaceReason = new AtomicReference<>();
    private final AtomicReference<String> knownClusterId = new AtomicReference<>();
    private final AtomicLong acked = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong consecutiveFailures = new AtomicLong();
    private volatile String lastError = "";
    private volatile int instance = 0;
    private long lastReplaceMs = System.currentTimeMillis();

    void run() {
        System.out.printf("%s producer start topic=%s run_id=%s bootstrap=%s%n", Common.now(), topic, runId, bootstrap);
        log.line(String.join("\t", Common.now(), topic, "-", "-", "-", "START run_id=" + runId, "-"));
        ScheduledExecutorService bg = Executors.newScheduledThreadPool(2);
        bg.scheduleAtFixedRate(this::report, 10, 10, TimeUnit.SECONDS);
        startClusterIdWatcher(bg);

        KafkaProducer<String, String> producer = create();
        long seq = 0;
        long intervalNs = 1_000_000_000L / ratePerSec;
        long next = System.nanoTime();
        while (true) {
            String reason = replaceReason.getAndSet(null);
            if (reason == null && consecutiveFailures.get() > 30L * ratePerSec
                    && System.currentTimeMillis() - lastReplaceMs > 60_000) {
                reason = "stalled consecutive_failures=" + consecutiveFailures.get() + " last=" + lastError;
            }
            if (reason != null) {
                producer = replace(producer, reason);
            }
            final long s = seq++;
            final int inst = instance;
            String value = "{\"run_id\":\"" + runId + "\",\"topic\":\"" + topic + "\",\"seq\":" + s
                    + ",\"ts\":\"" + Common.now() + "\",\"payload\":\"" + "x".repeat(100) + "\"}";
            ProducerRecord<String, String> rec = new ProducerRecord<>(topic, String.valueOf(s % 12), value);
            rec.headers().add("run_id", runId.getBytes(StandardCharsets.UTF_8));
            try {
                producer.send(rec, (md, ex) -> {
                    if (ex == null) {
                        acked.incrementAndGet();
                        consecutiveFailures.set(0);
                        log.line(String.join("\t", Common.now(), topic, Long.toString(s),
                                Integer.toString(md.partition()), Long.toString(md.offset()), "ACKED", "p" + inst));
                    } else {
                        onFailure(s, inst, ex);
                    }
                });
            } catch (TimeoutException e) {
                onFailure(s, inst, e); // max.block.ms: no metadata (topic missing / cluster down)
            } catch (Exception e) {
                onFailure(s, inst, e);
                replaceReason.compareAndSet(null, "send-exception " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            next += intervalNs;
            long sleepNs = next - System.nanoTime();
            if (sleepNs > 0) {
                Common.sleep(sleepNs / 1_000_000);
            } else {
                next = System.nanoTime(); // fell behind (blocked send) – don't burst afterwards
            }
        }
    }

    private void onFailure(long seq, int inst, Exception ex) {
        failed.incrementAndGet();
        consecutiveFailures.incrementAndGet();
        lastError = ex.getClass().getSimpleName() + ": " + ex.getMessage();
        log.line(String.join("\t", Common.now(), topic, Long.toString(seq), "-", "-",
                "FAILED:" + ex.getClass().getSimpleName(), "p" + inst));
        if (ex instanceof OutOfOrderSequenceException || ex instanceof UnknownProducerIdException
                || ex instanceof InvalidProducerEpochException || ex instanceof ProducerFencedException
                || ex instanceof IllegalStateException) {
            replaceReason.compareAndSet(null, "fatal " + ex.getClass().getSimpleName());
        }
    }

    private KafkaProducer<String, String> create() {
        Properties p = Common.baseConfig(bootstrap, "CLIENT_ID", "CLIENT_SECRET");
        p.put("client.id", "producer-" + topic + "-" + (++instance));
        p.put("acks", "all");
        p.put("enable.idempotence", "true");
        p.put("delivery.timeout.ms", "30000");
        p.put("request.timeout.ms", "10000");
        p.put("max.block.ms", "10000");
        p.put("linger.ms", "5");
        p.put("key.serializer", StringSerializer.class.getName());
        p.put("value.serializer", StringSerializer.class.getName());
        while (true) {
            try {
                return new KafkaProducer<>(p); // fails if the bootstrap service DNS is temporarily unresolvable
            } catch (Exception e) {
                System.out.printf("%s producer construction failed (%s) – retrying in 5 s%n", Common.now(), e.getMessage());
                Common.sleep(5000);
            }
        }
    }

    private KafkaProducer<String, String> replace(KafkaProducer<String, String> old, String reason) {
        int oldInstance = instance;
        try {
            old.close(java.time.Duration.ZERO); // pending records fail -> logged as FAILED (never acked)
        } catch (Exception e) {
            System.out.println("close failed: " + e);
        }
        KafkaProducer<String, String> fresh = create();
        lastReplaceMs = System.currentTimeMillis();
        consecutiveFailures.set(0);
        String msg = "PRODUCER_RECREATED reason=" + reason + " old=p" + oldInstance + " new=p" + instance
                + " cluster=" + knownClusterId.get();
        System.out.println(Common.now() + " " + msg);
        log.line(String.join("\t", Common.now(), topic, "-", "-", "-", msg, "p" + instance));
        return fresh;
    }

    /** Broker IDs and hostnames are reused by the recreated primary – detect the new cluster explicitly. */
    private void startClusterIdWatcher(ScheduledExecutorService bg) {
        Properties p = Common.baseConfig(bootstrap, "CLIENT_ID", "CLIENT_SECRET");
        p.put("client.id", "producer-" + topic + "-cluster-watch");
        p.put("request.timeout.ms", "4000");
        p.put("default.api.timeout.ms", "4000");
        AtomicReference<Admin> admin = new AtomicReference<>();
        AtomicLong adminFailures = new AtomicLong();
        bg.scheduleWithFixedDelay(() -> {
            try {
                if (admin.get() == null) admin.set(Admin.create(p));
                String id = admin.get().describeCluster().clusterId().get(4, TimeUnit.SECONDS);
                adminFailures.set(0);
                String prev = knownClusterId.getAndSet(id);
                if (prev == null) {
                    System.out.printf("%s cluster id %s%n", Common.now(), id);
                } else if (!prev.equals(id)) {
                    log.line(String.join("\t", Common.now(), topic, "-", "-", "-",
                            "CLUSTER_ID_CHANGED old=" + prev + " new=" + id, "p" + instance));
                    replaceReason.set("cluster-id-change old=" + prev + " new=" + id);
                }
            } catch (Exception e) {
                if (adminFailures.incrementAndGet() % 12 == 0 && admin.get() != null) {
                    admin.getAndSet(null).close(java.time.Duration.ZERO); // fresh bootstrap after ~1 min
                }
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    private void report() {
        System.out.printf("%s topic=%s instance=p%d acked=%d failed=%d consecutive_failures=%d cluster=%s last_error=%s%n",
                Common.now(), topic, instance, acked.get(), failed.get(), consecutiveFailures.get(),
                knownClusterId.get(), consecutiveFailures.get() > 0 ? lastError : "-");
    }
}
