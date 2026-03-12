/** Valid SQLite bind parameter value types. */
export type SqlValue = string | number | bigint | Buffer | Uint8Array | null;

// Extract named parameter names ($foo, :bar, @baz) from a SQL string literal at the type level.

type ParamPrefix = '$' | ':' | '@';

type AlphaNum =
  | 'a'|'b'|'c'|'d'|'e'|'f'|'g'|'h'|'i'|'j'|'k'|'l'|'m'|'n'|'o'|'p'|'q'|'r'|'s'|'t'|'u'|'v'|'w'|'x'|'y'|'z'
  | 'A'|'B'|'C'|'D'|'E'|'F'|'G'|'H'|'I'|'J'|'K'|'L'|'M'|'N'|'O'|'P'|'Q'|'R'|'S'|'T'|'U'|'V'|'W'|'X'|'Y'|'Z'
  | '0'|'1'|'2'|'3'|'4'|'5'|'6'|'7'|'8'|'9'|'_';

// Take consecutive alphanumeric chars from the start of S to form a parameter name.
type TakeParamName<S extends string, Acc extends string = ''> =
  S extends `${infer C}${infer Rest}`
    ? C extends AlphaNum
      ? TakeParamName<Rest, `${Acc}${C}`>
      : Acc extends '' ? never : Acc
    : Acc extends '' ? never : Acc;

// Skip consecutive alphanumeric chars, return the remainder.
type SkipParamName<S extends string> =
  S extends `${infer C}${infer Rest}`
    ? C extends AlphaNum ? SkipParamName<Rest> : S
    : '';

// Recurse through SQL, collecting parameter names that follow a prefix character.
type ExtractParams<SQL extends string> =
  SQL extends `${string}${ParamPrefix}${infer Rest}`
    ? TakeParamName<Rest> | ExtractParams<SkipParamName<Rest>>
    : never;

type HasNamedParams<SQL extends string> = ExtractParams<SQL> extends never ? false : true;

// Detect whether the SQL contains any positional `?` parameter.
type HasPositionalParams<SQL extends string> =
  SQL extends `${string}?${infer _}` ? true : false;

/**
 * Derive the bind-parameter signature from a SQL string literal.
 *
 * - Non-literal `string`: permissive `...any[]` (backwards compatible)
 * - Literal with named params (`$foo`, `:bar`, `@baz`): require `{ foo: unknown, bar: unknown, baz: unknown }`
 * - Literal with positional `?` params (no named): `...unknown[]`
 * - Literal with no params at all: `[]` (no arguments accepted)
 */
export type BindParams<SQL extends string> =
  string extends SQL
    ? [...params: any[]]
    : HasNamedParams<SQL> extends true
      ? [params: { [K in ExtractParams<SQL>]: SqlValue }]
      : HasPositionalParams<SQL> extends true
        ? [...params: (SqlValue | SqlValue[])[]]
        : [];
