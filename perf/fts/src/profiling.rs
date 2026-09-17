use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{Result, ensure};

pub fn start(enabled: bool) -> Result<Option<pprof::ProfilerGuard<'static>>> {
    if !enabled {
        return Ok(None);
    }
    let builder = pprof::ProfilerGuardBuilder::default().frequency(99);
    #[cfg(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64"
    ))]
    let builder = builder.blocklist(&["libc", "libgcc", "pthread", "vdso"]);
    Ok(Some(builder.build()?))
}

pub fn finish(profiler: Option<pprof::ProfilerGuard<'static>>, path: Option<&Path>) -> Result<()> {
    let Some(profiler) = profiler else {
        assert!(path.is_none());
        return Ok(());
    };
    let report = profiler.report().build()?;
    drop(profiler);
    let path = path.expect("enabled profiler must have an output path");
    ensure!(
        !report.data.is_empty(),
        "no CPU samples collected; increase --queries or --seconds"
    );
    let mut stacks = BTreeMap::<String, isize>::new();
    for (frames, count) in &report.data {
        let mut names = vec![frames.thread_name_or_id()];
        for frame in frames.frames.iter().rev() {
            for symbol in frame.iter().rev() {
                names.push(symbol.name().replace(';', ":"));
            }
        }
        *stacks.entry(names.join(";")).or_default() += count;
    }
    let mut folded = BufWriter::new(File::create_new(path.with_extension("folded"))?);
    for (stack, count) in stacks {
        writeln!(folded, "{stack} {count}")?;
    }
    folded.flush()?;
    report.flamegraph(File::create_new(path.with_extension("svg"))?)?;
    eprintln!(
        "profile {}: {} CPU samples",
        path.display(),
        report.data.values().sum::<isize>()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_worker_cpu_and_preserves_sample_counts() -> Result<()> {
        use std::time::{Duration, Instant};

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("workers");
        finish(start(false)?, None)?;
        let profiler = start(true)?;
        std::thread::scope(|scope| {
            for _ in 0..2 {
                scope.spawn(|| {
                    let start = Instant::now();
                    while start.elapsed() < Duration::from_secs(1) {
                        for value in 0..1000_u64 {
                            std::hint::black_box(value.wrapping_mul(31));
                        }
                    }
                });
            }
        });
        finish(profiler, Some(&path))?;
        let folded = std::fs::read_to_string(path.with_extension("folded"))?;
        let count: usize = folded
            .lines()
            .map(|line| line.rsplit_once(' ').unwrap().1.parse::<usize>().unwrap())
            .sum();
        assert!(count > 0);
        assert!(folded.contains("records_worker_cpu_and_preserves_sample_counts"));
        let svg = std::fs::read_to_string(path.with_extension("svg"))?;
        assert!(svg.contains("<svg"));
        let total = svg.split("<title>all (").nth(1).unwrap();
        let total = total.split(" samples,").next().unwrap().replace(',', "");
        assert_eq!(total.parse::<usize>()?, count);
        Ok(())
    }
}
