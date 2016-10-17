#!/usr/bin/env perl
use 5.014;
use strict;
use warnings;

use Kafka::Librd;

my $kafka = Kafka::Librd->new(
    Kafka::Librd::RD_KAFKA_PRODUCER,
    {
        #debug => 'generic,cgrp,topic,fetch',
    },
);

my $added = $kafka->brokers_add('localhost:9092');
say "Added $added brokers";

my $topic1 = $kafka->topic('test1',{});
my $topic2 = $kafka->topic('test2',{});

my $msg = 'aaa';

for (1..10) {
    my $err = $topic1->produce(-1, 0, $msg,$msg);
    $err and die "Couldn't produce: ", Kafka::Librd::Error::to_string($err);
    $err = $topic2->produce(-1, 0, $msg,$msg);
    $err and die "Couldn't produce: ", Kafka::Librd::Error::to_string($err);
    $msg++;
}

sleep 1 while $kafka->outq_len;

$kafka = undef;

my $res = Kafka::Librd::rd_kafka_wait_destroyed(5000);
say "Some kafka resources are still allocated: $res" if $res;
