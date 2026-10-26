mod cli_runtime;

#[cfg(all(feature = "jemalloc", target_os = "linux"))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc", target_os = "linux"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> anyhow::Result<()> {
    cli_runtime::build()?.block_on(brewfs::cli::run())
}
