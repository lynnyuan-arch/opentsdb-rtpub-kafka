package net.opentsdb.tsd.rtpublisher;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * @author lynn
 * @ClassName net.opentsdb.tsd.rtpublisher.TestKafkaRTPublisher
 * @Description TODO
 * @Date 19-6-4 上午9:02
 * @Version 1.0
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, KafkaRTPubPluginConfig.class, KafkaProducer.class})
@PowerMockIgnore("javax.management.*")
public class TestKafkaRTPublisher {

    private TSDB tsdb;
    private Config config;

    @Before
    public void before() throws Exception {
        tsdb = PowerMockito.mock(TSDB.class);
        config = new Config(false);

        config.overrideConfig("tsd.rtpublisher.kafka.bootstrap.servers", "localhost:9200");
        when(tsdb.getConfig()).thenReturn(config);
    }

    @Test
    public void initialize() throws Exception {
        config.overrideConfig("tsd.rtpublisher.kafka.client.id", "client-1");
        config.overrideConfig("tsd.rtpublisher.kafka.partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        config.overrideConfig("tsd.rtpublisher.kafka.compression.type", "none");
        config.overrideConfig("tsd.rtpublisher.kafka.connections.max.idle.ms", "540000");
        config.overrideConfig("send.buffer.bytes", "131072");
        config.overrideConfig("receive.buffer.bytes", "32768");
        config.overrideConfig("request.timeout.ms", "30000");
        config.overrideConfig("reconnect.backoff.max.ms", "1000");
        config.overrideConfig("reconnect.backoff.ms", "50");
        config.overrideConfig("tsd.rtpublisher.kafka.retries", "1");
        config.overrideConfig("tsd.rtpublisher.kafka.retry.backoff.ms", "100");
        config.overrideConfig("tsd.rtpublisher.kafka.enable.idempotence", "false");

        final KafkaRTPublisher plugin = new KafkaRTPublisher();
        plugin.initialize(tsdb);

        assertEquals("localhost:9200",  plugin.getProperties().getProperty("bootstrap.servers"));
        assertEquals("client-1",  plugin.getProperties().getProperty("client.id"));
        assertEquals( "org.apache.kafka.clients.producer.internals.DefaultPartitioner",plugin.getProperties().getProperty("partitioner.class"));
        assertEquals("none",  plugin.getProperties().getProperty("compression.type"));
        assertEquals(540000, ((Long)plugin.getProperties().get("connections.max.idle.ms")).longValue());
        assertEquals(1,  ((Integer)plugin.getProperties().get("retries")).intValue());
        assertEquals(100,  ((Long)plugin.getProperties().get("retry.backoff.ms")).longValue());
        assertEquals(false, Boolean.getBoolean(plugin.getProperties().getProperty("enable.idempotence")));
    }

    @Test (expected = IllegalArgumentException.class)
    public void initializeNullBrokers() throws Exception {
        config.overrideConfig("tsd.rtpublisher.kafka.bootstrap.servers", null);
        final KafkaRTPublisher plugin = new KafkaRTPublisher();
        plugin.initialize(tsdb);
    }

    @Test (expected = IllegalArgumentException.class)
    public void initializeNullTopic() throws Exception {
        config.overrideConfig("tsd.rtpublisher.kafka.datapoint.topic", null);
        final KafkaRTPublisher plugin = new KafkaRTPublisher();
        plugin.initialize(tsdb);
    }

}
