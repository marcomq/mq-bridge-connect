#include "bridge.h"

static const mqbrp_api_v1 MQBRP_API_V1 = {
    sizeof(mqbrp_api_v1),
    1,
    0,
    mqbrp_go_probe,
    mqbrp_go_bytes_free,
};

const mqbrp_api_v1 *mqbrp_get_api_v1(void) {
    return &MQBRP_API_V1;
}

