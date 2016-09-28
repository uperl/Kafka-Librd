#include "rdkafkaxs.h"

SV* krd_to_obj(void* krd) {
        return sv_setref_pv(newSV(0), "Kafka::Librd", krd);
}

SV* msg_to_obj(void* message) {
        return sv_setref_pv(newSV(0), "Kafka::Librd::Message", message);
}

void krd_call_dr_msg_cb(
        rd_kafka_t* rk,
        const rd_kafka_message_t* rkmessage,
        void* opaque) {
    dSP;
    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    EXTEND(SP,2);
    PUSHs(sv_2mortal(krd_to_obj(opaque)));
    PUSHs(sv_2mortal(msg_to_obj((void*)rkmessage)));
    PUTBACK;

    rdkafka_t* krd = (rdkafka_t*) opaque;
    call_sv(krd->dr_msg_cb, G_VOID);

    FREETMPS;
    LEAVE;
}

void krd_call_consume_cb(
        rd_kafka_message_t* rkmessage,
        void* opaque) {
    dSP;
    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    EXTEND(SP,2);
    PUSHs(sv_2mortal(krd_to_obj(opaque)));
    PUSHs(sv_2mortal(msg_to_obj((void*)rkmessage)));
    PUTBACK;

    rdkafka_t* krd = (rdkafka_t*) opaque;
    call_sv(krd->consume_cb, G_VOID);

    FREETMPS;
    LEAVE;
}

void krd_call_rebalance_cb(
        rd_kafka_t *rk,
        rd_kafka_resp_err_t err,
        rd_kafka_topic_partition_list_t *partitions,
        void *opaque) {
    // TODO
}

void krd_call_error_cb(
        rd_kafka_t *rk,
        int err,
        const char *reason,
        void* opaque) {
    // TODO
}

void krd_call_log_cb(
        const rd_kafka_t *rk,
        int level,
        const char *fac,
        const char *buf) {
    // TODO
}

#define ADDCALLBACK(name) if (!SvROK(val) || strncmp(sv_reftype(SvRV(val), 0), "CODE", 5) != 0) {\
    strncpy(errstr, #name " must be a code reference", 1024);\
    goto CROAK;\
}\
krd->name = SvRV(val);\
rd_kafka_conf_set_ ## name(krdconf, krd_call_ ## name);

rd_kafka_conf_t* krd_parse_config(rdkafka_t *krd, HV* params) {
    char errstr[1024];
    rd_kafka_conf_t* krdconf;
    rd_kafka_conf_res_t res;
    HE *he;

    krdconf = rd_kafka_conf_new();
    rd_kafka_conf_set_opaque(krdconf, (void *)krd);
    hv_iterinit(params);
    while (he = hv_iternext(params)) {
        STRLEN len;
        char* key = HePV(he, len);
        SV* val = HeVAL(he);
        if (strncmp(key, "dr_msg_cb", len) == 0) {
            ADDCALLBACK(dr_msg_cb);
        } else if (strncmp(key, "consume_cb", len) == 0) {
            ADDCALLBACK(consume_cb);
        } else if (strncmp(key, "rebalance_cb", len) == 0) {
            ADDCALLBACK(rebalance_cb);
        } else if (strncmp(key, "error_cb", len) == 0) {
            ADDCALLBACK(error_cb);
        } else if (strncmp(key, "log_cb", len) == 0) {
            ADDCALLBACK(log_cb);
        } else {
            // set named configuration property
            char *strval = SvPV(val, len);
            res = rd_kafka_conf_set(
                    krdconf,
                    key,
                    strval,
                    errstr,
                    1024);
            if (res != RD_KAFKA_CONF_OK)
                goto CROAK;
        }
    }

    return krdconf;

CROAK:
    rd_kafka_conf_destroy(krdconf);
    croak(errstr);
    return NULL;
}
