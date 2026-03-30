$ErrorActionPreference = "Stop"

if ($args.Count -gt 0) {
    cargo run -p limbo_sim -- @args
}
else {
    Write-Host "Running limbo_sim in infinite loop..."
    while ($true) {
        cargo run -p limbo_sim
    }
}