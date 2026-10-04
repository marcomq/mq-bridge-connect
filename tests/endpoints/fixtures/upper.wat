;; The module behind the `processor_wasm` case: uppercases the ASCII letters of
;; a message. Rebuild with `npx -y -p wabt wat2wasm upper.wat -o upper.wasm`.
(module
  (import "benthos_wasm" "v0_msg_as_bytes" (func $as_bytes (result i64)))
  (import "benthos_wasm" "v0_msg_set_bytes" (func $set_bytes (param i32 i32)))
  (memory (export "memory") 2)

  ;; The host copies the message here. One message at a time, so one address
  ;; does; a payload has to fit the 64 KiB page above it.
  (func (export "allocate") (param i32) (result i32)
    (i32.const 65536))

  (func (export "process")
    (local $packed i64) (local $ptr i32) (local $len i32) (local $i i32) (local $byte i32)
    (local.set $packed (call $as_bytes))
    (local.set $ptr (i32.wrap_i64 (i64.shr_u (local.get $packed) (i64.const 32))))
    (local.set $len (i32.wrap_i64 (local.get $packed)))
    (block $done
      (loop $next
        (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
        (local.set $byte (i32.load8_u (i32.add (local.get $ptr) (local.get $i))))
        (if (i32.lt_u (i32.sub (local.get $byte) (i32.const 97)) (i32.const 26))
          (then (i32.store8
            (i32.add (local.get $ptr) (local.get $i))
            (i32.sub (local.get $byte) (i32.const 32)))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $next)))
    (call $set_bytes (local.get $ptr) (local.get $len))))
