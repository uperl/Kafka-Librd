#include <librdkafka/rdkafka.h>
#include <EXTERN.h>
#include <perl.h>

typedef struct rdkafka_s {
    rd_kafka_t* rk;
    SV* dr_msg_cb;
    SV* consume_cb;
    SV* rebalance_cb;
    SV* error_cb;
    SV* log_cb;
    IV thx;
} rdkafka_t;

rd_kafka_conf_t* krd_parse_config(rdkafka_t* krd, HV* params);
