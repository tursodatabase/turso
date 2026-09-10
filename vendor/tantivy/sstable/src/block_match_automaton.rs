use tantivy_fst::Automaton;

/// Returns whether a block can match an automaton based on its bounds.
///
/// start key is exclusive, and optional to account for the first block. end key is inclusive and
/// mandatory.
pub(crate) fn can_block_match_automaton(
    start_key_opt: Option<&[u8]>,
    end_key: &[u8],
    automaton: &impl Automaton,
) -> bool {
    let start_key = if let Some(start_key) = start_key_opt {
        start_key
    } else {
        // if start_key_opt is None, we would allow an automaton matching the empty string to match
        if automaton.is_match(&automaton.start()) {
            return true;
        }
        &[]
    };
    can_block_match_automaton_with_start(start_key, end_key, automaton)
}

// similar to can_block_match_automaton, ignoring the edge case of the initial block
fn can_block_match_automaton_with_start(
    start_key: &[u8],
    end_key: &[u8],
    automaton: &impl Automaton,
) -> bool {
    // notation: in loops, we use `kb` to denotate a key byte (a byte taken from the start/end key),
    // and `rb`, a range byte (usually all values higher than a `kb` when comparing with
    // start_key, or all values lower than a `kb` when comparing with end_key)

    if start_key >= end_key {
        return false;
    }

    let common_prefix_len = crate::common_prefix_len(start_key, end_key);

    let mut base_state = automaton.start();
    for kb in &start_key[0..common_prefix_len] {
        base_state = automaton.accept(&base_state, *kb);
    }

    // this is not required for correctness, but allows dodging more expensive checks
    if !automaton.can_match(&base_state) {
        return false;
    }

    // we have 3 distinct case:
    // - keys are `abc` and `abcd` => we test for abc[\0-d].*
    // - keys are `abcd` and `abce` => we test for abc[d-e].*
    // - keys are `abcd` and `abc` => contradiction with start_key < end_key.
    //
    // ideally for (abc, abcde] we could test for abc([\0-c].*|d([\0-d].*|e)?)
    // but let's start simple (and correct), and tighten our bounds latter
    //
    // and for (abcde, abcfg] we could test for abc(d(e.+|[f-\xff].*)|e.*|f([\0-f].*|g)?)
    // abc (
    //  d(e.+|[f-\xff].*) |
    //  e.* |
    //  f([\0-f].*|g)?
    // )
    //
    // these are all written as regex, but can be converted to operations we can do:
    // - [x-y] is a for c in x..=y
    // - .* is a can_match()
    // - .+ is a for c in 0..=255 { accept(c).can_match() }
    // - ? is a the thing before can_match(), or current state.is_match()
    // - | means test both side

    // we have two cases, either start_key is a prefix of end_key (e.g. (abc, abcjp]),
    // or it is not (e.g. (abcdg, abcjp]). It is not possible however that end_key be a prefix of
    // start_key (or that both are equal) because we already handled start_key >= end_key.
    //
    // if we are in the first case, we want to visit the following states:
    // abc (
    //   [\0-i].* |
    //   j (
    //     [\0-o].* |
    //     p
    //   )?
    // )
    // Everything after `abc` is handled by `match_range_end`
    //
    // if we are in the 2nd case, we want to visit the following states:
    // abc (
    //   d(g.+|[h-\xff].*) | // this is handled by match_range_start
    //
    //   [e-i].* |           // this is handled here
    //
    //   j (                 // this is handled by match_range_end (but countrary to the other
    //    [\0-o].* |         // case, j is already consumed so to not check [\0-i].* )
    //    p
    //   )?
    // )

    let Some(start_range) = start_key.get(common_prefix_len) else {
        return match_range_end(&end_key[common_prefix_len..], &automaton, base_state);
    };

    let end_range = end_key[common_prefix_len];

    // things starting with start_range were handled in match_range_start
    // this starting with end_range are handled below.
    // this can run for 0 iteration in cases such as (abc, abd]
    for rb in (start_range + 1)..end_range {
        let new_state = automaton.accept(&base_state, rb);
        if automaton.can_match(&new_state) {
            return true;
        }
    }

    let state_for_start = automaton.accept(&base_state, *start_range);
    if match_range_start(
        &start_key[common_prefix_len + 1..],
        &automaton,
        state_for_start,
    ) {
        return true;
    }

    let state_for_end = automaton.accept(&base_state, end_range);
    if automaton.is_match(&state_for_end) {
        return true;
    }
    match_range_end(&end_key[common_prefix_len + 1..], &automaton, state_for_end)
}

fn match_range_start<S, A: Automaton<State = S>>(
    start_key: &[u8],
    automaton: &A,
    mut state: S,
) -> bool {
    // case (abcdgj, abcpqr], `abcd` is already consumed, we need to handle:
    // - [h-\xff].*
    // - g[k-\xff].*
    // - gj.+ == gf[\0-\xff].*

    for kb in start_key {
        // this is an optimisation, and is not needed for correctness
        if !automaton.can_match(&state) {
            return false;
        }

        // does the [h-\xff].* part. we skip if kb==255 as [\{0100}-\xff] is an empty range, and
        // this would overflow in our u8 world
        if *kb < u8::MAX {
            for rb in (kb + 1)..=u8::MAX {
                let temp_state = automaton.accept(&state, rb);
                if automaton.can_match(&temp_state) {
                    return true;
                }
            }
        }
        // push g
        state = automaton.accept(&state, *kb);
    }

    // this isn't required for correctness, but can save us from looping 256 below
    if !automaton.can_match(&state) {
        return false;
    }

    // does the final `.+`, which is the same as `[\0-\xff].*`
    for rb in 0..=u8::MAX {
        let temp_state = automaton.accept(&state, rb);
        if automaton.can_match(&temp_state) {
            return true;
        }
    }
    false
}

fn match_range_end<S, A: Automaton<State = S>>(
    end_key: &[u8],
    automaton: &A,
    mut state: S,
) -> bool {
    // for (abcdef, abcmps]. the prefix `abcm` has been consumed, `[d-l].*` was handled elsewhere,
    // we just need to handle
    // - [\0-o].*
    // - p
    // - p[\0-r].*
    // - ps
    for kb in end_key {
        // this is an optimisation, and is not needed for correctness
        if !automaton.can_match(&state) {
            return false;
        }

        // does the `[\0-o].*`
        for rb in 0..*kb {
            let temp_state = automaton.accept(&state, rb);
            if automaton.can_match(&temp_state) {
                return true;
            }
        }

        // push p
        state = automaton.accept(&state, *kb);
        // verify the `p` case
        if automaton.is_match(&state) {
            return true;
        }
    }
    false
}

#[cfg(test)]
pub(crate) mod tests {
    use proptest::prelude::*;
    use tantivy_fst::Automaton;

    use super::*;

    pub(crate) struct EqBuffer(pub Vec<u8>);

    impl Automaton for EqBuffer {
        type State = Option<usize>;

        fn start(&self) -> Self::State {
            Some(0)
        }

        fn is_match(&self, state: &Self::State) -> bool {
            *state == Some(self.0.len())
        }

        fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
            state
                .filter(|pos| self.0.get(*pos) == Some(&byte))
                .map(|pos| pos + 1)
        }

        fn can_match(&self, state: &Self::State) -> bool {
            state.is_some()
        }

        fn will_always_match(&self, _state: &Self::State) -> bool {
            false
        }
    }

    fn gen_key_strategy() -> impl Strategy<Value = Vec<u8>> {
        // we only generate bytes in [0, 1, 2, 254, 255] to reduce the search space without
        // ignoring edge cases that might ocure with integer over/underflow
        proptest::collection::vec(prop_oneof![0u8..=2, 254u8..=255], 0..5)
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 10000, .. ProptestConfig::default()
        })]

        #[test]
        fn test_proptest_automaton_match_block(start in gen_key_strategy(), end in gen_key_strategy(), key in gen_key_strategy()) {
            let expected = start < key && end >= key;
            let automaton = EqBuffer(key);

            assert_eq!(can_block_match_automaton(Some(&start), &end, &automaton), expected);
        }

        #[test]
        fn test_proptest_automaton_match_first_block(end in gen_key_strategy(), key in gen_key_strategy()) {
            let expected = end >= key;
            let automaton = EqBuffer(key);
            assert_eq!(can_block_match_automaton(None, &end, &automaton), expected);
        }
    }
}
