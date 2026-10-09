#import <Foundation/Foundation.h>

// The Rust-owned client runtime (#134). Strings returned here, including an
// `error_out` message, are freed with axton_mobile_free. `wake` runs on a
// runtime thread with `context`; keep `context` alive until
// axton_mobile_runtime_detach has returned.
typedef void (*axton_wake)(uint64_t runtime, void *context);
uint64_t axton_mobile_runtime_open(const char *request_json, axton_wake wake, void *context, char **error_out);
int32_t axton_mobile_runtime_submit(uint64_t runtime, const char *message_json, char **error_out);
char *axton_mobile_runtime_drain(uint64_t runtime);
void axton_mobile_runtime_detach(uint64_t runtime);
void axton_mobile_free(char *output);
