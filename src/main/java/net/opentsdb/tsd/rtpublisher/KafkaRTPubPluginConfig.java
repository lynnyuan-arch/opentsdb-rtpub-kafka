package net.opentsdb.tsd.rtpublisher;

import net.opentsdb.utils.Config;

import java.util.Map;

/**
 * @author lynn
 * @ClassName net.opentsdb.tsd.rtpublisher.KafkaRTPubPluginConfig
 * @Description TODO
 * @Date 19-6-3 下午4:12
 * @Version 1.0
 **/
public class KafkaRTPubPluginConfig extends Config {

    public KafkaRTPubPluginConfig(Config parent) {
        super(parent);
    }

    @Override
    protected void setDefaults() {
        default_map.put("tsd.rtpublisher.kafka.bootstrap.servers", "");
        default_map.put("tsd.rtpublisher.kafka.datapoint.enable", "true");
        default_map.put("tsd.rtpublisher.kafka.datapoint.topic", "datapoint");
        default_map.put("tsd.rtpublisher.kafka.annotation.enable", "false");
        default_map.put("tsd.rtpublisher.kafka.annotation.topic", "annotation");
        default_map.put("tsd.rtpublisher.kafka.acks", "all");
        default_map.put("tsd.rtpublisher.kafka.key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        default_map.put("tsd.rtpublisher.kafka.value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        default_map.put("tsd.rtpublisher.kafka.batch.size", "16384");
        default_map.put("tsd.rtpublisher.kafka.linger.ms", "100");
        default_map.put("tsd.rtpublisher.kafka.buffer.memory", "33554432");

        default_map.put("tsd.rtpublisher.kafka.flush.number", "500");
        default_map.put("tsd.rtpublisher.kafka.flush.ms", "2000");

        for (Map.Entry<String, String> entry : default_map.entrySet()) {
            if (!properties.containsKey(entry.getKey()))
                properties.put(entry.getKey(), entry.getValue());
        }
    }
}
