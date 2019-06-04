package net.opentsdb.tsd.rtpublisher;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.utils.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lynn
 * @ClassName net.opentsdb.tsd.rtpublisher.KafkaRTPublisher
 * @Description TODO
 * @Date 19-6-3 下午3:02
 * @Version 1.0
 **/
public class KafkaRTPublisher extends RTPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRTPublisher.class);

    private TSDB tsdb;
    private KafkaRTPubPluginConfig config = null;

    private KafkaProducer<byte[], byte[]> producer;

    private Boolean datapointEnable;
    private String datapointTopic;

    private Boolean annotationEnable;
    private String annotationTopic;

    private Integer flushNumber;
    private Long flushMs;

    private Properties properties = null;
    private static AtomicLong lastTimestamp = new AtomicLong(0);
    private static AtomicInteger records = new AtomicInteger(0);

    private static AtomicLong datapointRecords = new AtomicLong(0);
    private static AtomicLong annotaionRecords = new AtomicLong(0);

    @Override
    public void initialize(TSDB tsdb) {
        this.tsdb = tsdb;
        this.config = new KafkaRTPubPluginConfig(tsdb.getConfig());

        String servers = config.getString("tsd.rtpublisher.kafka.bootstrap.servers");
        if(Strings.isNullOrEmpty(servers)){
            throw new IllegalArgumentException(
                    "Missing config 'tsd.rtpublisher.kafka.bootstrap.servers'");
        }

        this.datapointEnable = config.getBoolean("tsd.rtpublisher.kafka.datapoint.enable");
        if(!datapointEnable){
            throw new IllegalArgumentException(
                    "'tsd.rtpublisher.kafka.datapoint.enabled' must be true");
        }
        this.datapointTopic = config.getString("tsd.rtpublisher.kafka.datapoint.topic");
        if(Strings.isNullOrEmpty(datapointTopic)){
            throw new IllegalArgumentException(
                    "Missing config 'tsd.rtpublisher.kafka.datapoint.topic'");
        }

        this.annotationEnable = config.getBoolean("tsd.rtpublisher.kafka.annotation.enable");
        if(annotationEnable){
            this.annotationTopic = config.getString("tsd.rtpublisher.kafka.annotation.topic");
            if(Strings.isNullOrEmpty(annotationTopic)){
                throw new IllegalArgumentException(
                        "Missing config 'tsd.rtpublisher.kafka.annotation.topic'");
            }
        }

        if(null == this.properties){
            this.properties = new Properties();
        }
        properties.put("bootstrap.servers", config.getString("tsd.rtpublisher.kafka.bootstrap.servers"));
        properties.put("acks", config.getString("tsd.rtpublisher.kafka.acks"));
        properties.put("key.serializer", config.getString("tsd.rtpublisher.kafka.key.serializer"));
        properties.put("value.serializer", config.getString("tsd.rtpublisher.kafka.value.serializer"));
        properties.put("batch.size", config.getInt("tsd.rtpublisher.kafka.batch.size"));
        properties.put("linger.ms", config.getLong("tsd.rtpublisher.kafka.linger.ms"));
        properties.put("buffer.memory", config.getLong("tsd.rtpublisher.kafka.buffer.memory"));

        this.flushMs = config.getLong("tsd.rtpublisher.kafka.flush.ms");
        this.flushNumber = config.getInt("tsd.rtpublisher.kafka.flush.number");

        setProperties();

        producer = new KafkaProducer<byte[], byte[]>(properties);

        LOG.info("initialize KafkaRTPublisher Plugin.");
    }

    private void setProperties(){
        setStringProperty("partitioner.class");
        setStringProperty("client.id");
        setStringProperty("compression.type");
        setLongProperty("connections.max.idle.ms");
        setIntProperty("retries");
        setLongProperty("retry.backoff.ms");
        setLongProperty("send.buffer.bytes");
        setLongProperty("receive.buffer.bytes");
        setLongProperty("request.timeout.ms");
        setLongProperty("reconnect.backoff.max.ms");
        setLongProperty("reconnect.backoff.ms");
        setBoolProperty("enable.idempotence");

        setStringProperty("sasl.client.callback.handler.class");
        setStringProperty("sasl.jaas.config");
        setStringProperty("/usr/bin/kinit");
        setLongProperty("sasl.kerberos.min.time.before.relogin");
        setStringProperty("sasl.kerberos.service.name");
        setFloatProperty("sasl.kerberos.ticket.renew.jitter");
        setFloatProperty("sasl.kerberos.ticket.renew.window.factor");

        setStringProperty("sasl.login.callback.handler.class");
        setStringProperty("sasl.login.class");
        setLongProperty("sasl.login.refresh.buffer.seconds");
        setLongProperty("sasl.login.refresh.buffer.seconds");
        setLongProperty("sasl.login.refresh.min.period.seconds");
        setFloatProperty("sasl.login.refresh.window.factor");
        setFloatProperty("sasl.login.refresh.window.jitter");
        setStringProperty("sasl.mechanism");
        setStringProperty("security.protocol");
        setLongProperty("send.buffer.bytes");
        setStringProperty("ssl.cipher.suites");
        setStringProperty("ssl.enabled.protocols"); //[TLSv1.2, TLSv1.1, TLSv1]
        setStringProperty("ssl.endpoint.identification.algorithm");
        setStringProperty("ssl.key.password");
        setStringProperty("ssl.keymanager.algorithm");
        setStringProperty("ssl.keystore.location");
        setStringProperty("ssl.keystore.password");
        setStringProperty("ssl.keystore.type");
        setStringProperty("ssl.protocol");
        setStringProperty("ssl.provider");
        setStringProperty("ssl.secure.random.implementation");
        setStringProperty("ssl.trustmanager.algorithm");
        setStringProperty("ssl.truststore.location");
        setStringProperty("ssl.truststore.password");
        setStringProperty("ssl.truststore.type");
        setLongProperty("transaction.timeout.ms");
        setStringProperty("transactional.id");
    }

    @Override
    public Deferred<Object> shutdown() {
        if (producer != null) {
            producer.close();
        }
        return Deferred.fromResult(null);
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void collectStats(StatsCollector collector) {
        collector.record("rtpublisher.kafka.datapoints",  datapointRecords.get());
        collector.record("rtpublisher.kafka.annotations",  annotaionRecords.get());
    }

    @Override
    public Deferred<Object> publishDataPoint(String metric, long timestamp, long value, Map<String, String> tags, byte[] tsuid) {
        Map<String, Object> map = Maps.newHashMap();
        map.put("metric", metric);
        map.put("timestamp", timestamp);
        map.put("tags", tags);
        map.put("tsuid", tsuid);
        map.put("value", value);

        datapointRecords.getAndIncrement();
        return send2Kafka(datapointTopic, tsuid, JSON.serializeToBytes(map));
    }

    @Override
    public Deferred<Object> publishDataPoint(String metric, long timestamp, double value, Map<String, String> tags, byte[] tsuid) {
        Map<String, Object> map = Maps.newHashMap();
        map.put("metric", metric);
        map.put("timestamp", timestamp);
        map.put("tags", tags);
        map.put("tsuid", tsuid);
        map.put("value", value);

        datapointRecords.getAndIncrement();
        return send2Kafka(datapointTopic, tsuid, JSON.serializeToBytes(map));
    }

    @Override
    public Deferred<Object> publishAnnotation(Annotation annotation) {
        if(annotationEnable){
            annotaionRecords.getAndIncrement();
            return send2Kafka(annotationTopic, annotation.getTSUID().getBytes(), JSON.serializeToBytes(annotation));
        }
        return Deferred.fromResult(null);
    }

    public Properties getProperties() {
        return properties;
    }

    /**
     *
     * @param topic
     * @param key
     * @param value
     * @return
     */
    private Deferred<Object> send2Kafka(String topic, byte[] key, byte[] value){
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, value);

        producer.send(record, (metadata, exception) -> {
            if(null != exception){
                Deferred.fromError(exception);
            }
        });
        if(records.incrementAndGet() >= flushNumber
            || System.currentTimeMillis() - lastTimestamp.get() >= flushMs){
            LOG.debug("flush {} to kafka broker.", records.get());
            producer.flush();
            records.set(0);
            lastTimestamp.set(System.currentTimeMillis());
        }

        return Deferred.fromResult(null);
    }

    /**
     *
     * @param key
     */
    private void setStringProperty(String key){
        if(config.hasProperty("tsd.rtpublisher.kafka." + key)  && null != properties){
            properties.put(key, config.getString("tsd.rtpublisher.kafka." + key));
        }
    }

    /**
     *
     * @param key
     */
    private void setIntProperty(String key){
        if(config.hasProperty("tsd.rtpublisher.kafka." + key)  && null != properties){
            properties.put(key, config.getInt("tsd.rtpublisher.kafka." + key));
        }
    }

    /**
     *
     * @param key
     */
    private void setLongProperty(String key){
        if(config.hasProperty("tsd.rtpublisher.kafka." + key) && null != properties){
            properties.put(key, config.getLong("tsd.rtpublisher.kafka." + key));
        }
    }

    /**
     *
     * @param key
     */
    private void setBoolProperty(String key){
        if(config.hasProperty("tsd.rtpublisher.kafka." + key)  && null != properties){
            properties.put(key, config.getBoolean("tsd.rtpublisher.kafka." + key));
        }
    }

    private void setFloatProperty(String key){
        if(config.hasProperty("tsd.rtpublisher.kafka." + key)  && null != properties){
            properties.put(key, config.getFloat("tsd.rtpublisher.kafka." + key));
        }
    }
}
