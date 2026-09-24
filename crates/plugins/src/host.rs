//! WASM plugin host. Guest modules must export:
//!   - `memory`               (linear memory, for passing bytes across the boundary)
//!   - `alloc(len: i32) -> i32`        (bump-allocate `len` bytes, return the pointer)
//!   - `transform(ptr: i32, len: i32) -> i64`  (packed (out_ptr << 32) | out_len)
//!
//! The host serializes an owned `Event` to JSON, writes it into guest
//! memory via `alloc`, calls `transform`, then reads the result back out
//! of the pointer/length the guest returned. JSON over a hand-rolled
//! alloc/memory ABI is a deliberately simple wire format â€” a real
//! production system would likely use a binary format here, but JSON
//! keeps the ABI itself (the actually interesting part) easy to verify.

use domain::{DomainError, Event, Transform};
use wasmtime::{Engine, Linker, Module, Store, TypedFunc};

pub struct WasmTransform {
    name: String,
    engine: Engine,
    module: Module,
}

struct HostState;

impl WasmTransform {
    pub fn load(name: impl Into<String>, wasm_bytes: &[u8]) -> anyhow::Result<Self> {
        let engine = Engine::default();
        let module = Module::new(&engine, wasm_bytes)?;
        Ok(WasmTransform { name: name.into(), engine, module })
    }

    fn run(&self, input_json: &[u8]) -> anyhow::Result<Vec<u8>> {
        let mut store = Store::new(&self.engine, HostState);
        let linker: Linker<HostState> = Linker::new(&self.engine);
        let instance = linker.instantiate(&mut store, &self.module)?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| anyhow::anyhow!("wasm module has no exported 'memory'"))?;
        let alloc: TypedFunc<u32, u32> = instance.get_typed_func(&mut store, "alloc")?;
        let transform_fn: TypedFunc<(u32, u32), u64> =
            instance.get_typed_func(&mut store, "transform")?;

        let in_len = input_json.len() as u32;
        let in_ptr = alloc.call(&mut store, in_len)?;
        memory.write(&mut store, in_ptr as usize, input_json)?;

        let packed = transform_fn.call(&mut store, (in_ptr, in_len))?;
        let out_ptr = (packed >> 32) as u32;
        let out_len = (packed & 0xFFFF_FFFF) as u32;

        let mut out_buf = vec![0u8; out_len as usize];
        memory.read(&mut store, out_ptr as usize, &mut out_buf)?;
        Ok(out_buf)
    }
}

impl Transform for WasmTransform {
    fn name(&self) -> &str {
        &self.name
    }

    fn apply<'a>(&self, event: &Event<'a>) -> Result<Vec<Event<'static>>, DomainError> {
        let owned = event.clone().into_owned();
        let input_json = serde_json::to_vec(&vec![owned])
            .map_err(|e| DomainError::TransformFailed(self.name.clone(), e.to_string()))?;
        let out_bytes = self
            .run(&input_json)
            .map_err(|e| DomainError::TransformFailed(self.name.clone(), e.to_string()))?;
        serde_json::from_slice(&out_bytes)
            .map_err(|e| DomainError::TransformFailed(self.name.clone(), e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal guest module written directly in WAT (no wasm32 toolchain
    /// needed to test this). It implements the exact ABI above but does
    /// the simplest possible thing: returns the same ptr/len it was
    /// given, i.e. echoes the input bytes straight back. That's enough
    /// to prove the full round trip â€” alloc, memory write, call, memory
    /// read â€” actually moves real bytes through a real sandboxed guest.
    const ECHO_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (global $heap_ptr (mut i32) (i32.const 1024))
          (func (export "alloc") (param $len i32) (result i32)
            (local $ptr i32)
            global.get $heap_ptr
            local.set $ptr
            global.get $heap_ptr
            local.get $len
            i32.add
            global.set $heap_ptr
            local.get $ptr)
          (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
            local.get $ptr
            i64.extend_i32_u
            i64.const 32
            i64.shl
            local.get $len
            i64.extend_i32_u
            i64.or))
    "#;

    #[test]
    fn wasm_echo_transform_round_trips_through_linear_memory() {
        let wasm_bytes = wat::parse_str(ECHO_WAT).expect("valid WAT");
        let transform = WasmTransform::load("echo", &wasm_bytes).expect("module loads");

        let event = Event::borrowed(42, 1000, "sensor-1", b"raw-bytes");
        let out = transform.apply(&event).expect("wasm call succeeds");

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].stream_id, 42);
        assert_eq!(out[0].key, "sensor-1");
        assert_eq!(&*out[0].payload, b"raw-bytes" as &[u8]);
    }
}