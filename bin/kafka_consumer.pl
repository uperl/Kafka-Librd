#!/usr/bin/env perl
use 5.014;
use strict;
use warnings;

use Kafka::Librd;
use Getopt::Long;

GetOptions(
    "group-id=s" => \my $group_id,
    "topic=s"    => \my @topics,
    "brokers=s"  => \my $brokers,
    "debug"      => \my $debug,
);

$group_id //= "test-consumer";
$brokers  //= "localhost:9092";

my $kafka = Kafka::Librd->new(
    Kafka::Librd::RD_KAFKA_CONSUMER,
    {
        'group.id' => $group_id,
        ( $debug ? ( debug => 'cgrp,topic,fetch' ) : () ),
    },
);

my $added = $kafka->brokers_add($brokers);
say "Added $added brokers";

my $err = $kafka->subscribe( \@topics );
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

$kafka->destroy;

Kafka::Librd::rd_kafka_wait_destroyed(5000);
