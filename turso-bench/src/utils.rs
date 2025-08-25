use std::time::{SystemTime, UNIX_EPOCH};

/// Get current time in microseconds since Unix epoch
pub fn get_current_utime() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// Get relative time from a start time in microseconds
pub fn get_relative_utime(start: u64) -> u64 {
    get_current_utime().saturating_sub(start)
}

/// Format duration for display
pub fn format_duration(duration: std::time::Duration) -> String {
    let total_seconds = duration.as_secs();
    let microseconds = duration.subsec_micros();
    
    if total_seconds > 0 {
        format!("{} sec {}us", total_seconds, microseconds)
    } else {
        format!("{}us", duration.as_micros())
    }
}

/// Calculate throughput in operations per second
pub fn calculate_throughput(operations: u64, duration: std::time::Duration) -> f64 {
    if duration.as_secs_f64() > 0.0 {
        operations as f64 / duration.as_secs_f64()
    } else {
        0.0
    }
}

/// Format bytes for display (B, KB, MB, GB)
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    const THRESHOLD: u64 = 1024;

    if bytes < THRESHOLD {
        return format!("{} B", bytes);
    }

    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= THRESHOLD as f64 && unit_index < UNITS.len() - 1 {
        size /= THRESHOLD as f64;
        unit_index += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_index])
}

/// Generate random string for database operations
pub fn generate_random_string(length: usize) -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::thread_rng();
    
    (0..length)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Progress bar helper
pub struct ProgressBar {
    total: u64,
    current: u64,
    width: usize,
    quiet: bool,
}

impl ProgressBar {
    pub fn new(total: u64, quiet: bool) -> Self {
        Self {
            total,
            current: 0,
            width: 50,
            quiet,
        }
    }

    pub fn update(&mut self, current: u64) {
        if self.quiet {
            return;
        }

        self.current = current;
        let percentage = if self.total > 0 {
            (self.current * 100) / self.total
        } else {
            0
        };

        let filled = if self.total > 0 {
            (self.current * self.width as u64) / self.total
        } else {
            0
        };

        let bar: String = (0..self.width)
            .map(|i| if i < filled as usize { '█' } else { '░' })
            .collect();

        print!("\r[{}] {}%", bar, percentage);
        
        if current >= self.total {
            println!();
        }
    }

    pub fn finish(&self) {
        if !self.quiet {
            println!();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
    }

    #[test]
    fn test_calculate_throughput() {
        let duration = std::time::Duration::from_secs(2);
        let throughput = calculate_throughput(100, duration);
        assert_eq!(throughput, 50.0);
    }

    #[test]
    fn test_generate_random_string() {
        let s1 = generate_random_string(10);
        let s2 = generate_random_string(10);
        
        assert_eq!(s1.len(), 10);
        assert_eq!(s2.len(), 10);
        // Very unlikely to be the same
        assert_ne!(s1, s2);
    }
}