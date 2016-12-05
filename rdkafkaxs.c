#include "rdkafkaxs.h"

#define ERRSTR_SIZE 1024

rd_kafka_conf_t* krd_parse_config(pTHX_ rdkafka_t *krd, HV* params) {
    char errstr[ERRSTR_SIZE];
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
        if (strncmp(key, "default_topic_config", len) == 0) {
            if (!SvROK(val) || strncmp(sv_reftype(SvRV(val), 0), "HASH", 5) != 0) {
                strncpy(errstr, "default_topic_config must be a hash reference", ERRSTR_SIZE);
                goto CROAK;
            }
            rd_kafka_topic_conf_t* topconf = krd_parse_topic_config(aTHX_ (HV*)SvRV(val), errstr);
            if (topconf == NULL) goto CROAK;
            rd_kafka_conf_set_default_topic_conf(krdconf, topconf);
        } else {
            // set named configuration property
            char *strval = SvPV(val, len);
            res = rd_kafka_conf_set(
                    krdconf,
                    key,
                    strval,
                    errstr,
                    ERRSTR_SIZE);
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

rd_kafka_topic_conf_t* krd_parse_topic_config(pTHX_ HV *params, char* errstr) {
    rd_kafka_topic_conf_t* topconf = rd_kafka_topic_conf_new();
    rd_kafka_conf_res_t res;
    HE *he;

    hv_iterinit(params);
    while (he = hv_iternext(params)) {
        STRLEN len;
        char* key = HePV(he, len);
        SV* val = HeVAL(he);
        char *strval = SvPV(val, len);
        res = rd_kafka_topic_conf_set(
                topconf,
                key,
                strval,
                errstr,
                ERRSTR_SIZE);
        if (res != RD_KAFKA_CONF_OK)
            goto ERROR;
    }

    return topconf;

ERROR:
    rd_kafka_topic_conf_destroy(topconf);
    return NULL;
}
