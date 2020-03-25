<?php
return [
    'kafka_consmer_addr' => env('KAFKA_CONSUMER_ADDR', 'localhost:9092'),
    'kafka_producer_addr' => env('KAFKA_PRODUCER_ADDR', 'localhost:9092'),
    'kafka_consumer_topic_prefix' => env('KAFKA_CONSUMER_TOPIC_PREFIX', 'icleaner'),
    'kafka_producer_topic_prefix' => env('KAFKA_PRODUCER_TOPIC_PREFIX', 'icleaner'),
    'kafka_consumer_group' => env('KAFKA_CONSUMER_GROUP', 'ICleanerConsumerGroup'),
    'run_project' => env('RUN_PROJECT', 0),
    'kafka_topic_rule' => '%s_%s_%s',
    'kafka_consumer_time' => 150000,
    'kafka_consumer_fail_job' => '%s_%s_consumer_fail_obj',
    'kafka_producer_fail_job' => '%s_%s_producer_fail_obj',
    'kafka_topic_job' => '%s_%s_topic_job',
    'kafka_topic_fail_job' => '%s_%s_topic_fail_job',
    'kafka_test_env' => env('KAFKA_TEST_ENV', false),
    'queue_max_timeout' => 5,
    'queue_max_times' => env('QUEUE_MAX_TIMES', 22),
    'topic_rule' => '%s_%s_%s',
    'rdkafka_producer_config' => [
        'metadata.broker.list' => ['val' => '', 'func' => 'getBrokerList'], 
        'socket.keepalive.enable' => ['val' => 'true'], 
        'log.connection.close' => ['val' => 'false']
    ],
    'rdkafka_consumer_config' => [
        // Configure the group.id. All consumer with the same group.id will come
        // different partitions
        'group.id' => ['val' => '', 'func' => 'getGroupId'],
        // Initial list of Kafka brokers
        'metadata.broker.list' => ['val' => '', 'func' => 'getBrokerList'], 
        // Set where to start consuming messages when there is no initial offset in offset store or the desired offest is out of range.
        // 'smallest': start from the beginning
        'auto.offset.reset' => ['val' => 'smallest'],
        'log.connection.close' => ['val' => 'false'],
        // 'session.timeout.ms' => ['val' => '400000'],
        // 'max.partition.fetch.bytes' => ['val' => '848576']
    ]    
];