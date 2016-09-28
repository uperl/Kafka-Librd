#include <librdkafka/rdkafka.h>
#include <EXTERN.h>
#include <perl.h>

typedef struct rdkafka_s {
    rd_kafka_t* rk;
    CV* dr_msg_cb;
    CV* consume_cb;
    CV* rebalance_cb;
    CV* error_cb;
    CV* log_cb;
    IV thx;
} rdkafka_t;

rd_kafka_conf_t* krd_parse_config(rdkafka_t* krd, HV* params);
