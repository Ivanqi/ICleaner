<?php
return [
    'kafka_addr' => env('KAFKA_ADDR', 'localhost:9092'),
    'kafka_topic_prefix' => env('KAFKA_TOPIC_PREFIX', 'icleaner'),
    'kafka_consumer_group' => env('KAFKA_CONSUMER_GROUP', 'ICleanerConsumerGroup'),
    'run_project' => env('RUN_PROJECT', 0),
    'kafka_topic_rule' => '%s_%s_%s',
    'kafka_consumer_time' => 120 * 10000
];