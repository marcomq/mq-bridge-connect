#ifndef MQBRP_BRIDGE_H
#define MQBRP_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

#define MQBRP_OK 0
#define MQBRP_INVALID 2
#define MQBRP_INTERNAL 3
#define MQBRP_END_OF_STREAM 4

#define MQBRP_KIND_CONSUMER 0
#define MQBRP_KIND_PUBLISHER 1

#define MQBRP_ACK 0
#define MQBRP_NACK 1

typedef struct mqbrp_owned_bytes {
    uint8_t *ptr;
    size_t len;
} mqbrp_owned_bytes;

typedef int32_t (*mqbrp_probe_fn)(uint32_t behavior, mqbrp_owned_bytes *error_out);
typedef void (*mqbrp_bytes_free_fn)(mqbrp_owned_bytes value);

typedef int32_t (*mqbrp_stream_open_fn)(uint32_t kind, uint8_t *config, size_t config_len,
                                        uint64_t *handle_out, mqbrp_owned_bytes *error_out);
typedef int32_t (*mqbrp_stream_next_batch_fn)(uint64_t handle, uint32_t max_messages,
                                              uint32_t timeout_ms, uint64_t *batch_id_out,
                                              mqbrp_owned_bytes *batch_out,
                                              mqbrp_owned_bytes *error_out);
typedef int32_t (*mqbrp_stream_commit_fn)(uint64_t handle, uint64_t batch_id,
                                          uint8_t *dispositions, size_t count,
                                          mqbrp_owned_bytes *error_out);
typedef int32_t (*mqbrp_stream_publish_fn)(uint64_t handle, uint8_t *batch, size_t batch_len,
                                           mqbrp_owned_bytes *error_out);
typedef int32_t (*mqbrp_stream_close_fn)(uint64_t handle, uint32_t timeout_ms,
                                         mqbrp_owned_bytes *error_out);

typedef struct mqbrp_api_v1 {
    size_t struct_size;
    uint16_t abi_major;
    uint16_t abi_minor;
    mqbrp_probe_fn probe;
    mqbrp_bytes_free_fn bytes_free;
    mqbrp_stream_open_fn stream_open;
    mqbrp_stream_next_batch_fn stream_next_batch;
    mqbrp_stream_commit_fn stream_commit;
    mqbrp_stream_publish_fn stream_publish;
    mqbrp_stream_close_fn stream_close;
} mqbrp_api_v1;

const mqbrp_api_v1 *mqbrp_get_api_v1(void);
int32_t mqbrp_go_probe(uint32_t behavior, mqbrp_owned_bytes *error_out);
void mqbrp_go_bytes_free(mqbrp_owned_bytes value);
int32_t mqbrp_go_stream_open(uint32_t kind, uint8_t *config, size_t config_len,
                             uint64_t *handle_out, mqbrp_owned_bytes *error_out);
int32_t mqbrp_go_stream_next_batch(uint64_t handle, uint32_t max_messages, uint32_t timeout_ms,
                                   uint64_t *batch_id_out, mqbrp_owned_bytes *batch_out,
                                   mqbrp_owned_bytes *error_out);
int32_t mqbrp_go_stream_commit(uint64_t handle, uint64_t batch_id, uint8_t *dispositions,
                               size_t count, mqbrp_owned_bytes *error_out);
int32_t mqbrp_go_stream_publish(uint64_t handle, uint8_t *batch, size_t batch_len,
                                mqbrp_owned_bytes *error_out);
int32_t mqbrp_go_stream_close(uint64_t handle, uint32_t timeout_ms, mqbrp_owned_bytes *error_out);

#endif
