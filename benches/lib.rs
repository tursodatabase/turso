//! Benchmarks for the Turso engine. The benchmarks are the bench targets
//! of this crate; the library holds code they share.

/// Number of calls in one CodSpeed measurement of a divan benchmark.
#[cfg(feature = "codspeed")]
const CODSPEED_BATCH: usize = 100;

/// The bencher that `#[turso_macros::divan_bench]` functions receive.
///
/// codspeed-divan-compat measures a single call. For a function of a few hundred
/// instructions, one cache miss more or less (for example because unrelated code
/// moved in the binary) changes the result by 30%. So in CodSpeed builds one
/// measurement calls the benchmarked closure `CODSPEED_BATCH` times. Only the
/// divan methods that can be batched this way are offered.
pub struct Bencher<'a, 'b>(divan::Bencher<'a, 'b>);

impl<'a, 'b> Bencher<'a, 'b> {
    pub fn new(bencher: divan::Bencher<'a, 'b>) -> Self {
        Self(bencher)
    }

    pub fn bench_local<O, B: FnMut() -> O>(self, benched: B) {
        // The outputs are returned so that they are dropped after the measurement
        // ends, as divan does with the output of a single call.
        #[cfg(feature = "codspeed")]
        {
            let mut benched = benched;
            self.0
                .with_inputs(|| Vec::with_capacity(CODSPEED_BATCH))
                .bench_local_values(move |mut outputs: Vec<O>| {
                    for _ in 0..CODSPEED_BATCH {
                        outputs.push(benched());
                    }
                    outputs
                });
        }
        #[cfg(not(feature = "codspeed"))]
        self.0.bench_local(benched);
    }

    pub fn with_inputs<I, G: FnMut() -> I>(self, gen_input: G) -> BencherWithInputs<'a, 'b, G> {
        BencherWithInputs {
            bencher: self.0,
            gen_input,
        }
    }
}

/// A [`Bencher`] that creates a new input for every call, outside the measurement.
pub struct BencherWithInputs<'a, 'b, G> {
    bencher: divan::Bencher<'a, 'b>,
    gen_input: G,
}

impl<'a, 'b, I, G: FnMut() -> I> BencherWithInputs<'a, 'b, G> {
    pub fn bench_local_values<O, B: FnMut(I) -> O>(self, benched: B) {
        #[cfg(feature = "codspeed")]
        {
            let mut benched = benched;
            let mut gen_input = self.gen_input;
            self.bencher
                .with_inputs(move || {
                    let inputs: Vec<I> = (0..CODSPEED_BATCH).map(|_| gen_input()).collect();
                    (inputs, Vec::with_capacity(CODSPEED_BATCH))
                })
                // Both vectors are returned so that their memory, and the outputs,
                // are freed after the measurement ends.
                .bench_local_values(move |(mut inputs, mut outputs): (Vec<I>, Vec<O>)| {
                    for input in inputs.drain(..) {
                        outputs.push(benched(input));
                    }
                    (inputs, outputs)
                });
        }
        #[cfg(not(feature = "codspeed"))]
        self.bencher
            .with_inputs(self.gen_input)
            .bench_local_values(benched);
    }
}
