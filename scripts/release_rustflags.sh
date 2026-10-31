# Source me: local release builds add packed relative relocations (DT_RELR,
# needs glibc >= 2.36 at load time) on top of .cargo/config.toml. CI wheel
# builds (manylinux_2_28) do not source this file. The env var replaces the
# config.toml rustflags for this target, so --icf=all is repeated here.
if [ "$(uname -s)-$(uname -m)" = "Linux-x86_64" ]; then
  export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C link-arg=-Wl,--icf=all -C link-arg=-Wl,-z,pack-relative-relocs"
fi
