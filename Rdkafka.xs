/* vim: set expandtab sts=4: */
#include <EXTERN.h>
#include <perl.h>
#include <XSUB.h>

#include "ppport.h"
#include "rdkafkaxs.h"

typedef struct rdkafka_s {
    rdkafka* rk;
    CV dr_msg_cb;
    CV consume_cb;
    CV rebalance_cb;
    CV error_cb;
    CV log_cb;
    IV thx;
} rdkafka_t;

MODULE = Kafka::Librd    PACKAGE = Kafka::Librd    PREFIX = krd_
PROTOTYPES: DISABLE

INCLUDE: const_xs.inc

int
krd_rd_kafka_version()
    CODE:
        RETVAL = rd_kafka_version(void);
    OUTPUT:
        RETVAL

char*
krd_rd_kafka_version_str()
    CODE:
        RETVAL = rd_kafka_version_str(void);
    OUTPUT:
        RETVAL

rdkafka_t*
krd__new(type, params)
        int type
        HV* params
    PREINIT:
        rd_kafka_conf_t* conf;
        rd_kafka_t* rk;
        char errstr[1024];
    CODE:
        Newx(RETVAL, 1, rdkafka_t);
        conf = krd_parse_config(RETVAL, params);
        rk = rd_kafka_new(type, conf, errstr, 1024);
        if (rk == NULL) {
            croak(errstr);
        }
        RETVAL->rk = rk;
        RETVAL->thx = (IV)PERL_GET_THX;
    OUTPUT:
        RETVAL

void
krd_DESTROY(rdk)
        rdkafka_t* rdk
    CODE:
        if (rdk->thx == (IV)PERL_GET_THX) {
            rd_kafka_destroy(rdk->rk);
            Safefree(rdk);
        }
