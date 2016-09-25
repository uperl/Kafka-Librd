#include <librdkafka/rdkafka.h>
#include <perl.h>

rd_kafka_conf_t* krd_parse_config(HV* params);
