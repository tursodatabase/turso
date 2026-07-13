use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

pub struct ProgressBars {
    num_ticks: usize,
    multi_progress: MultiProgress,
}

impl ProgressBars {
    /// `hidden` suppresses drawing entirely; used by multiprocess workers,
    /// whose bars would otherwise redraw over each other on the shared
    /// terminal.
    pub(crate) fn new(num_ticks: usize, hidden: bool) -> Self {
        let multi_progress = if hidden {
            MultiProgress::with_draw_target(ProgressDrawTarget::hidden())
        } else {
            MultiProgress::new()
        };
        Self {
            multi_progress,
            num_ticks,
        }
    }

    pub(crate) fn add(&self, id: String) -> Progress {
        let progress_bar = self
            .multi_progress
            .add(ProgressBar::new(self.num_ticks as u64));

        progress_bar.set_style(Self::style());
        progress_bar.set_prefix(id);

        Progress::new(progress_bar)
    }

    fn style() -> ProgressStyle {
        ProgressStyle::default_bar()
            .template(
                "[{elapsed_precise}] {prefix} {bar:40.cyan/blue} {pos:>7}/{len:7} ({percent}%) {msg}",
            )
            .unwrap()
            .progress_chars("##-")
    }
}

#[derive(Clone)]
pub struct Progress {
    progress_bar: ProgressBar,
}

impl Progress {
    fn new(progress_bar: ProgressBar) -> Self {
        progress_bar.set_message("executing queries...");

        Self { progress_bar }
    }

    pub fn tick(&mut self) {
        self.progress_bar.inc(1);
    }

    pub fn finish(&mut self) {
        self.progress_bar.finish_with_message("done");
    }
}
