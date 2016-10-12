#include <librdkafka/rdkafka.h>
#include <EXTERN.h>
#include <perl.h>

typedef struct rdkafka_s {
    rd_kafka_t* rk;
    IV thx;
} rdkafka_t;

rd_kafka_conf_t* krd_parse_config(rdkafka_t* krd, HV* params);
rd_kafka_topic_conf_t* krd_parse_topic_config(HV *params, char* errstr);
