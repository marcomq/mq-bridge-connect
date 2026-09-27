#include "bridge.h"

static const mqbrp_api_v1 MQBRP_API_V1 = {
    sizeof(mqbrp_api_v1),
    1,
    2,
    mqbrp_go_probe,
    mqbrp_go_bytes_free,
    mqbrp_go_stream_open,
    mqbrp_go_stream_next_batch,
    mqbrp_go_stream_commit,
    mqbrp_go_stream_publish,
    mqbrp_go_stream_close,
    mqbrp_go_processor_open,
    mqbrp_go_processor_apply,
    mqbrp_go_processor_close,
};

const mqbrp_api_v1 *mqbrp_get_api_v1(void) {
    return &MQBRP_API_V1;
}
