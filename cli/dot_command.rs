pub(crate) fn tokenize_dot_command(line: &str) -> (Vec<String>, Option<char>) {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut token_started = false;

    for ch in line.chars() {
        match quote {
            Some(delimiter) if ch == delimiter => quote = None,
            Some(_) => current.push(ch),
            None if ch.is_whitespace() => {
                if token_started {
                    args.push(std::mem::take(&mut current));
                    token_started = false;
                }
            }
            None if matches!(ch, '\'' | '"') && !token_started => {
                quote = Some(ch);
                token_started = true;
            }
            None => {
                current.push(ch);
                token_started = true;
            }
        }
    }

    if token_started {
        args.push(current);
    }

    (args, quote)
}

#[cfg(test)]
#[path = "tests/unit/dot_command/tests.rs"]
mod tests;
