/**
 * Represents a parameterized SQL query created using the `sql` tagged template.
 */
export class SqlQuery {
  readonly sql: string;
  readonly params: unknown[];

  constructor(sql: string, params: unknown[]) {
    this.sql = sql;
    this.params = params;
  }
}

/**
 * Tagged template literal for creating parameterized SQL queries.
 * Values are automatically converted to parameters, preventing SQL injection.
 *
 * @example
 * ```typescript
 * import { sql } from '@tursodatabase/database';
 *
 * const minAge = 18;
 * const name = 'Alice';
 *
 * // Simple parameterized query
 * const query = sql`SELECT * FROM users WHERE age > ${minAge}`;
 * // query.sql = "SELECT * FROM users WHERE age > ?"
 * // query.params = [18]
 *
 * // Multiple parameters
 * const query2 = sql`SELECT * FROM users WHERE name = ${name} AND age > ${minAge}`;
 * // query2.sql = "SELECT * FROM users WHERE name = ? AND age > ?"
 * // query2.params = ['Alice', 18]
 *
 * // Use with db.query() or db.queryAll()
 * const users = await db.queryAll(query);
 * ```
 */
export function sql(strings: TemplateStringsArray, ...values: unknown[]): SqlQuery {
  // Build the SQL string with ? placeholders
  let sqlStr = strings[0];
  for (let i = 0; i < values.length; i++) {
    sqlStr += '?' + strings[i + 1];
  }

  return new SqlQuery(sqlStr, values);
}

/**
 * Type guard to check if a value is a SqlQuery
 */
export function isSqlQuery(value: unknown): value is SqlQuery {
  return value instanceof SqlQuery;
}
