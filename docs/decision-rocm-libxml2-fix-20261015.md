# Fix: llama.cpp HIP build on the Radeon 780M (ROCm/lld libxml2)

**Status:** Resolved (2026-10-15) · **Owner:** opencode
**Affects:** HYDRA `.78`, ACHILLES `.11` (Ryzen 7 8700G, Radeon 780M iGPU, ROCm)

## Symptom

Building llama.cpp with `-DGGML_HIP=ON` aborts with:

```
clang++: error: unable to execute command: No such file or directory
clang++: error: amdgcn-link command failed due to signal (use -v to see invocation)
```

The server then crashes at first decode (`ggml_cuda_op_scale` → `ggml_abort`) —
because the *build* silently shipped a broken HIP backend.

## Root cause (not llama.cpp, not ROCm)

Deep in the CMake HIP-compiler probe:

```
/opt/rocm-6.4.1/lib/llvm/bin/lld: error while loading shared libraries:
  libxml2.so.2: cannot open shared object file: No such file or directory
```

- ACHILLES runs **Ubuntu 26.04**, whose libxml2 is **soname `.so.16`**.
- ROCm **6.4.1**'s `lld` (invoked as `amdgcn-link` for the AMDGPU device link) was
  built against the **old `libxml2.so.2`**.
- Soname mismatch → `lld` can't load → the device link fails → a broken HIP lib.

Adding an `amdgcn-link → lld` symlink alone does **not** fix it (lld still can't
load).

## Fix

Two things, both build-time:

1. **libxml2 compat shim** — put `.so.16` where lld looks for `.so.2`, and expose
   it via `LD_LIBRARY_PATH` (no system change):
   ```sh
   mkdir -p ~/rocm-compat
   ln -sf /usr/lib/x86_64-linux-gnu/libxml2.so.16 ~/rocm-compat/libxml2.so.2
   export LD_LIBRARY_PATH=$HOME/rocm-compat
   ```
   (A `no version information available` warning is harmless.)

2. **ROCm LLVM bin on PATH** so clang finds `amdgcn-link`/`lld`:
   ```sh
   export PATH="/opt/rocm-6.4.1/lib/llvm/bin:/opt/rocm-6.4.1/bin:$PATH"
   ```

Then configure + build:
```sh
cd ~/llama-build/llama.cpp
cmake -B build -DGGML_HIP=ON -DAMDGPU_TARGETS=gfx1102 \
      -DCMAKE_BUILD_TYPE=Release -DLLAMA_CURL=OFF -DBUILD_SHARED_LIBS=ON
cmake --build build --config Release -j16
```
Result: `CONFIG OK`, build 100 %, `bin/llama-server` produced.

## Verified

`gemma-4-E2B_q4_0-it.gguf` on the 780M (`HSA_OVERRIDE_GFX_VERSION=11.0.2 -ngl 99`):
- `2+2?` → `"content":"4"` ✅
- ~**51 tok/s** prompt, ~**20 tok/s** generation.

Launcher: `~/llama-scripts/serve-gemma4-arm.sh [model.gguf]` (port via `$PORT`).

## Replicate on HYDRA

Same steps. HYDRA's current llama.cpp predates the fix (build `07d9378`) and is
why its bge-m3 survives but a Gemma-4 graph crashes. Update source → rebuild with
the shim + PATH.

## Note

The `no version information available` warning means lld is loading a *newer*
libxml2 than it was linked against. That is fine for `lld`'s use (device linking
does not exercise libxml2's versioned API). If it ever misbehaves, the durable
alternative is to install a real `libxml2.so.2` (e.g. extract from an older
Ubuntu/Debian package) into `~/rocm-compat/` instead of symlinking `.so.16`.
