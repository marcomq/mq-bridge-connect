#ifndef MQBRP_BRIDGE_H
#define MQBRP_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

typedef struct mqbrp_owned_bytes {
    uint8_t *ptr;
    size_t len;
} mqbrp_owned_bytes;

typedef int32_t (*mqbrp_probe_fn)(uint32_t behavior, mqbrp_owned_bytes *error_out);
typedef void (*mqbrp_bytes_free_fn)(mqbrp_owned_bytes value);

typedef struct mqbrp_api_v1 {
    size_t struct_size;
    uint16_t abi_major;
    uint16_t abi_minor;
    mqbrp_probe_fn probe;
    mqbrp_bytes_free_fn bytes_free;
} mqbrp_api_v1;

const mqbrp_api_v1 *mqbrp_get_api_v1(void);
int32_t mqbrp_go_probe(uint32_t behavior, mqbrp_owned_bytes *error_out);
void mqbrp_go_bytes_free(mqbrp_owned_bytes value);

#endif

