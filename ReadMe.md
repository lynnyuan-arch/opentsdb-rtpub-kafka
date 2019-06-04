       ___                 _____ ____  ____  ____
      / _ \ _ __   ___ _ _|_   _/ ___||  _ \| __ )
     | | | | '_ \ / _ \ '_ \| | \___ \| | | |  _ \
     | |_| | |_) |  __/ | | | |  ___) | |_| | |_) |
      \___/| .__/ \___|_| |_|_| |____/|____/|____/
           |_|    The modern time series database.

 
RTPublisher Kafka plugin for OpenTSDB

##Installation
* Compile the plugin via ``mvn package -Pdist``.
* Create a plugins directory for your TSD
* Copy the plugin from the ``target`` directory into your TSD's plugin's directory.
* Add the following configs to your ``opentsdb.conf`` file.
    * Add ``tsd.core.plugin_path = <directory>`` pointing to a valid directory for your plugins.
    * Add ``tsd.rtpublisher.enable = true``
    * Add ``tsd.rtpublisher.plugin = net.opentsdb.tsd.rtpublisher.KafkaRTPublisher`` 
    * Add ``tsd.rtpublisher.kafka.bootstrap.servers = <host>:<port>`` The kafka broker listin the format ``<host1>[:port],<host2>[:port],``.
    * Add ``tsd.rtpublisher.kafka.datapoint.enable = true`` The default value of this item is ``true``.
    * Add ``tsd.rtpublisher.kafka.datapoint.topic = <topic-name>`` The kafka topic of annotation
* If you want to publish annotation info to kafka broker, Add the the following configs to your ``opentsdb.conf`` file.
    * Add ``tsd.rtpublisher.kafka.annotation.enable = true`` The default value of this item is ``false``.
    * Add ``tsd.rtpublisher.kafka.annotation.topic = <topic-name>`` The kafka topic of annotation.

* If you want to override other configuration items of kafka producer, Add the the following configs to your ``opentsdb.conf`` file.
Especially when the kafka brokers enabled security feature, the configuration items related with security must be overriden.
    * Add ``tsd.rtpublisher.kafka.acks = all`` The default value of this item is ``all`` 
    * Add ``tsd.rtpublisher.kafka.batch.size = 16384`` The default value of this item is ``16384`` .
    

## Introductions
* Serializer of key and value
    * Both key and value, we choose ``org.apache.kafka.common.serialization.ByteArraySerializer`` as serializer.
    * The key of datapoint and annotation is the tsuid.
    * The value of datapoint and annotation is converted to json string, then convert to byte array.
* Configuration items of producer flush
    * ``tsd.rtpublisher.kafka.flush.number = 500`` The records number of a batch.
    * ``tsd.rtpublisher.kafka.flush.ms = 200``  The time interval of a batch, unit is "millseconds".
    * When one of these two condition happen, the producer will flush the batch.

* Other Configuration items
    * ``tsd.rtpublisher.kafka.linger.ms = 100`` The default value of this item is ``100`` 
    * ``tsd.rtpublisher.kafka.buffer.memory = 33554432`` The default value of this item is ``33554432`` 

TODO - doc em
