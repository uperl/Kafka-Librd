/* vim: set expandtab sts=4: */
#include <EXTERN.h>
#include <perl.h>
#include <XSUB.h>

#include "ppport.h"
#include "rdkafkaxs.h"

MODULE = Kafka::Librd    PACKAGE = Kafka::Librd    PREFIX = krd_
PROTOTYPES: DISABLE

INCLUDE: const_xs.inc

int
krd_rd_kafka_version()
    CODE:
        RETVAL = rd_kafka_version();
    OUTPUT:
        RETVAL

const char*
krd_rd_kafka_version_str()
    CODE:
        RETVAL = rd_kafka_version_str();
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

int
krd_brokers_add(rdk, brokerlist)
        rdkafka_t* rdk
        char* brokerlist
    CODE:
        RETVAL = rd_kafka_brokers_add(rdk->rk, brokerlist);
    OUTPUT:
        RETVAL

int
krd_subscribe(rdk, topics)
        rdkafka_t* rdk
        AV* topics
    PREINIT:
        STRLEN strl;
        int i, len;
        rd_kafka_topic_partition_list_t* topic_list;
        char* topic;
        SV** topic_sv;
    CODE:
        len = av_len(topics) + 1;
        topic_list = rd_kafka_topic_partition_list_new(len);
        for (i=0; i < len; i++) {
            topic_sv = av_fetch(topics, i, 0);
            if (topic_sv != NULL) {
                topic = SvPV(*topic_sv, strl);
                rd_kafka_topic_partition_list_add(topic_list, topic, -1);
            }
        }
        RETVAL = rd_kafka_subscribe(rdk->rk, topic_list);
        rd_kafka_topic_partition_list_destroy(topic_list);
    OUTPUT:
        RETVAL

int
krd_unsubscribe(rdk)
        rdkafka_t* rdk
    CODE:
        RETVAL = rd_kafka_unsubscribe(rdk->rk);
    OUTPUT:
        RETVAL

rd_kafka_message_t*
krd_consumer_poll(rdk, timeout_ms)
        rdkafka_t* rdk
        int timeout_ms
    CODE:
        RETVAL = rd_kafka_consumer_poll(rdk->rk, timeout_ms);
    OUTPUT:
        RETVAL

int
krd_consumer_close(rdk)
        rdkafka_t* rdk
    CODE:
        RETVAL = rd_kafka_consumer_close(rdk->rk);
    OUTPUT:
        RETVAL

void
krd_DESTROY(rdk)
        rdkafka_t* rdk
    CODE:
        if (rdk->thx == (IV)PERL_GET_THX) {
            Safefree(rdk);
        }

int
krd_rd_kafka_wait_destroyed(timeout_ms)
        int timeout_ms
    CODE:
        RETVAL = rd_kafka_wait_destroyed(timeout_ms);
    OUTPUT:
        RETVAL

MODULE = Kafka::Librd    PACKAGE = Kafka::Librd::Message    PREFIX = krdm_
PROTOTYPES: DISABLE

SV*
krdm_payload(msg)
        rd_kafka_message_t* msg
    CODE:
        RETVAL = newSVpvn(msg->payload, msg->len);
    OUTPUT:
        RETVAL

SV*
krdm_key(msg)
        rd_kafka_message_t* msg
    CODE:
        RETVAL = newSVpvn(msg->key, msg->key_len);
    OUTPUT:
        RETVAL

void
krdm_DESTROY(msg)
        rd_kafka_message_t* msg
    CODE:
        rd_kafka_message_destroy(msg);

MODULE = Kafka::Librd    PACKAGE = Kafka::Librd::Error    PREFIX = krde_
PROTOTYPES: DISABLE

HV*
krde_rd_kafka_get_err_descs()
    PREINIT:
        const struct rd_kafka_err_desc* descs;
        size_t cnt;
        int i;
    CODE:
        rd_kafka_get_err_descs(&descs, &cnt);
        RETVAL = newHV();
        for (i = 0; i < cnt; i++) {
            if (descs[i].name != NULL) {
                hv_store(RETVAL, descs[i].name, strnlen(descs[i].name, 1024), newSViv(descs[i].code), 0);
            }
        }
    OUTPUT:
        RETVAL

const char*
krde_to_string(code)
        int code
    CODE:
        RETVAL = rd_kafka_err2str(code);
    OUTPUT:
        RETVAL

const char*
krde_to_name(code)
        int code
    CODE:
        RETVAL = rd_kafka_err2name(code);
    OUTPUT:
        RETVAL
