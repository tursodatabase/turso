use std::{
    io,
    process::{Command, ExitStatus},
    thread,
    time::{Duration, Instant},
};

#[derive(Debug)]
pub enum Outcome {
    Exited(ExitStatus),
    TimedOut,
}

pub fn run_child(command: &mut Command, timeout: Duration) -> io::Result<Outcome> {
    let mut child = command.spawn()?;
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(Outcome::Exited(status)),
            Ok(None) => {}
            Err(error) => {
                child.kill()?;
                child.wait()?;
                return Err(error);
            }
        }
        if start.elapsed() >= timeout {
            child.kill()?;
            child.wait()?;
            return Ok(Outcome::TimedOut);
        }
        thread::sleep(Duration::from_millis(10).min(timeout.saturating_sub(start.elapsed())));
    }
}
