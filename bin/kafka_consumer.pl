#!/usr/bin/env perl
use 5.014;
use strict;
use warnings;

use Kafka::Librd;

my $kafka = Kafka::Librd->new(
    Kafka::Librd::RD_KAFKA_CONSUMER,
    {
        'group.id' => 'test-consumer',

        #debug => 'generic,cgrp,topic,fetch',
    },
);

my $added = $kafka->brokers_add('localhost:9092');
say "Added $added brokers";

my $err = $kafka->subscribe( [ 'test1', 'test2', 'abcdef', ] );
if ( $err != 0 ) {
    die "Couldn't subscribe: ", Kafka::Librd::Error::to_string($err);
}
say "Subscribed";

my $stop;

$SIG{INT} = sub { say "Got SIGINT"; $stop = 1 };

while (1) {
    my $msg = $kafka->consumer_poll(1000);
    if ( defined $msg ) {
        my $err = $msg->err;
        say "-----";
        say "Error: ", Kafka::Librd::Error::to_name($err) if $err;
        say "Topic: ", $msg->topic;
        say "Part: ",  $msg->partition;
        say "Offset: ",  $msg->offset;
        say "Key: ",     $msg->key if defined $msg->key;
        say "Payload: ", $msg->payload;
    }
    last if $stop;
}

$kafka->consumer_close;

$kafka = undef;

Kafka::Librd::rd_kafka_wait_destroyed(5000);
