package net.opentsdb.tsd.rtpublisher;

import net.opentsdb.utils.Config;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author lynn
 * @ClassName net.opentsdb.tsd.rtpublisher.TestKafkaRTPubPluginConfig
 * @Description TODO
 * @Date 19-6-4 上午8:56
 * @Version 1.0
 **/
public class TestKafkaRTPubPluginConfig {

    @Test
    public void defaults() throws Exception {
        final KafkaRTPubPluginConfig config = new KafkaRTPubPluginConfig(new Config(false));

        assertEquals("", config.getString("tsd.rtpublisher.kafka.bootstrap.servers"));
        assertEquals(true, config.getBoolean("tsd.rtpublisher.kafka.datapoint.enable"));
        assertEquals("datapoint", config.getString("tsd.rtpublisher.kafka.datapoint.topic"));
        assertEquals(false, config.getBoolean("tsd.rtpublisher.kafka.annotation.enable"));
        assertEquals("annotation", config.getString("tsd.rtpublisher.kafka.annotation.topic"));

        assertEquals("all", config.getString("tsd.rtpublisher.kafka.acks"));
        assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", config.getString("tsd.rtpublisher.kafka.key.serializer"));
        assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", config.getString("tsd.rtpublisher.kafka.value.serializer"));

        assertEquals(16384, config.getInt("tsd.rtpublisher.kafka.batch.size"));

        assertEquals(16384, config.getInt("tsd.rtpublisher.kafka.batch.size"));
        assertEquals(100l, config.getLong("tsd.rtpublisher.kafka.linger.ms"));
        assertEquals(33554432l, config.getLong("tsd.rtpublisher.kafka.buffer.memory"));
        assertEquals(500, config.getInt("tsd.rtpublisher.kafka.flush.number"));
        assertEquals(2000l, config.getLong("tsd.rtpublisher.kafka.flush.ms"));

    }
}
