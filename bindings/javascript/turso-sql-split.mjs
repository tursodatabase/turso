const ReadState = Object.freeze({
    Invalid: 'Invalid',
    Start: 'Start',
    Normal: 'Normal',
    Explain: 'Explain',
    Create: 'Create',
    Trigger: 'Trigger',
    Semi: 'Semi',
    End: 'End',
});

const Token = Object.freeze({
    TkSemi: 'TkSemi',
    TkWhitespace: 'TkWhitespace',
    TkOther: 'TkOther',
    TkExplain: 'TkExplain',
    TkCreate: 'TkCreate',
    TkTemp: 'TkTemp',
    TkTrigger: 'TkTrigger',
    TkEnd: 'TkEnd',
});

function isAsciiWhitespace(char) {
    const code = char.charCodeAt(0);
    return code === 0x20 || code === 0x09 || code === 0x0a || code === 0x0b || code === 0x0c || code === 0x0d;
}

function isAsciiAlpha(char) {
    const code = char.charCodeAt(0);
    return (code >= 0x41 && code <= 0x5a) || (code >= 0x61 && code <= 0x7a);
}

function isAsciiAlnum(char) {
    const code = char.charCodeAt(0);
    return isAsciiAlpha(char) || (code >= 0x30 && code <= 0x39);
}

function classifyKeyword(word) {
    switch (word) {
        case 'EXPLAIN':
            return Token.TkExplain;
        case 'CREATE':
            return Token.TkCreate;
        case 'TEMP':
        case 'TEMPORARY':
            return Token.TkTemp;
        case 'TRIGGER':
            return Token.TkTrigger;
        case 'END':
            return Token.TkEnd;
        default:
            return Token.TkOther;
    }
}

function nextToken(sql, start) {
    let i = start;

    while (i < sql.length) {
        const char = sql[i];
        const nextChar = sql[i + 1];

        if (char === '\'' || char === '"' || char === '`' || char === '[') {
            const endChar = char === '[' ? ']' : char;
            i++;
            while (i < sql.length) {
                const current = sql[i];
                i++;
                if (current === endChar) {
                    break;
                }
            }
            continue;
        }

        if (char === '-' && nextChar === '-') {
            i += 2;
            while (i < sql.length && sql[i] !== '\n') {
                i++;
            }
            continue;
        }

        if (char === '/' && nextChar === '*') {
            i += 2;
            let sawStar = false;
            while (i < sql.length) {
                const current = sql[i];
                i++;
                if (sawStar && current === '/') {
                    break;
                }
                sawStar = current === '*';
            }
            continue;
        }

        if (char === ';') {
            return {
                token: Token.TkSemi,
                nextIndex: i + 1,
                tokenEnd: i + 1,
            };
        }

        if (isAsciiWhitespace(char)) {
            return {
                token: Token.TkWhitespace,
                nextIndex: i + 1,
                tokenEnd: i + 1,
            };
        }

        if (isAsciiAlpha(char) || char === '_') {
            let end = i + 1;
            while (end < sql.length && (isAsciiAlnum(sql[end]) || sql[end] === '_')) {
                end++;
            }

            return {
                token: classifyKeyword(sql.slice(i, end).toUpperCase()),
                nextIndex: end,
                tokenEnd: end,
            };
        }

        return {
            token: Token.TkOther,
            nextIndex: i + 1,
            tokenEnd: i + 1,
        };
    }

    return null;
}

function transition(state, token) {
    switch (state) {
        case ReadState.Invalid:
            switch (token) {
                case Token.TkSemi:
                    return ReadState.Start;
                case Token.TkWhitespace:
                    return ReadState.Invalid;
                case Token.TkExplain:
                    return ReadState.Explain;
                case Token.TkCreate:
                    return ReadState.Create;
                default:
                    return ReadState.Normal;
            }
        case ReadState.Start:
            switch (token) {
                case Token.TkSemi:
                    return ReadState.Start;
                case Token.TkWhitespace:
                    return ReadState.Start;
                case Token.TkExplain:
                    return ReadState.Explain;
                case Token.TkCreate:
                    return ReadState.Create;
                default:
                    return ReadState.Normal;
            }
        case ReadState.Normal:
            switch (token) {
                case Token.TkSemi:
                    return ReadState.Start;
                case Token.TkWhitespace:
                    return ReadState.Normal;
                default:
                    return ReadState.Normal;
            }
        case ReadState.Explain:
            switch (token) {
                case Token.TkSemi:
                    return ReadState.Start;
                case Token.TkWhitespace:
                    return ReadState.Explain;
                case Token.TkCreate:
                    return ReadState.Create;
                case Token.TkExplain:
                case Token.TkTemp:
                case Token.TkTrigger:
                case Token.TkEnd:
                    return ReadState.Normal;
                default:
                    return ReadState.Explain;
            }
        case ReadState.Create:
            switch (token) {
                case Token.TkSemi:
                    return ReadState.Start;
                case Token.TkWhitespace:
                    return ReadState.Create;
                case Token.TkTemp:
                    return ReadState.Create;
                case Token.TkTrigger:
                    return ReadState.Trigger;
                default:
                    return ReadState.Normal;
            }
        case ReadState.Trigger:
            switch (token) {
                case Token.TkSemi:
                    return ReadState.Semi;
                case Token.TkWhitespace:
                    return ReadState.Trigger;
                default:
                    return ReadState.Trigger;
            }
        case ReadState.Semi:
            switch (token) {
                case Token.TkSemi:
                    return ReadState.Semi;
                case Token.TkWhitespace:
                    return ReadState.Semi;
                case Token.TkEnd:
                    return ReadState.End;
                default:
                    return ReadState.Trigger;
            }
        case ReadState.End:
            switch (token) {
                case Token.TkSemi:
                    return ReadState.Start;
                case Token.TkWhitespace:
                    return ReadState.End;
                default:
                    return ReadState.Trigger;
            }
        default:
            return ReadState.Invalid;
    }
}

function hasMeaningfulSql(sql) {
    let index = 0;
    while (true) {
        const token = nextToken(sql, index);
        if (!token) {
            return false;
        }

        if (token.token !== Token.TkWhitespace && token.token !== Token.TkSemi) {
            return true;
        }

        index = token.nextIndex;
    }
}

/**
 * Split SQL text into individual statements using sqlite3_complete-like semantics.
 *
 * This matches testing/runner/src/parser/sql_complete.rs so CREATE TRIGGER bodies
 * containing semicolons are treated as a single statement until the ;END; sentinel.
 */
function splitStatements(sql) {
    const statements = [];
    let state = ReadState.Invalid;
    let statementStart = 0;
    let index = 0;

    while (true) {
        const tokenInfo = nextToken(sql, index);
        if (!tokenInfo) {
            break;
        }

        const newState = transition(state, tokenInfo.token);

        if (newState === ReadState.Start && state !== ReadState.Start) {
            const candidate = sql.slice(statementStart, tokenInfo.tokenEnd).trim();
            if (candidate && hasMeaningfulSql(candidate)) {
                statements.push(candidate);
            }
            statementStart = tokenInfo.tokenEnd;
        }

        state = newState;
        index = tokenInfo.nextIndex;
    }

    const remainder = sql.slice(statementStart).trim();
    if (remainder && hasMeaningfulSql(remainder)) {
        statements.push(remainder);
    }

    return statements;
}

export { splitStatements };
