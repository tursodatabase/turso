// Type declarations for @tursodatabase/pg-experimental.
// The API contract is specified in postgres/PGLITE.md and documented in
// postgres/js/docs/api.md.

export type Row<T = { [key: string]: any }> = T;

export type Results<T = { [key: string]: any }> = {
  rows: Row<T>[];
  fields: { name: string; dataTypeID: number }[];
  affectedRows?: number;
};

export type ParserOptions = { [pgType: number]: (value: string) => any };
export type SerializerOptions = { [pgType: number]: (value: any) => string };

export interface QueryOptions {
  rowMode?: "object" | "array";
  parsers?: ParserOptions;
  serializers?: SerializerOptions;
  paramTypes?: number[];
  /** Per-query timeout in milliseconds (Turso extension). */
  queryTimeout?: number;
}

export interface PGliteOptions {
  dataDir?: string;
  parsers?: ParserOptions;
  serializers?: SerializerOptions;
  debug?: number;
  /** Open the database in read-only mode (Turso extension). */
  readonly?: boolean;
  /** Throw instead of creating a missing database file (Turso extension). */
  fileMustExist?: boolean;
  /** Busy timeout in milliseconds (Turso extension). */
  timeout?: number;
  /** Default maximum query execution time in milliseconds (Turso extension). */
  defaultQueryTimeout?: number;
}

export interface Transaction {
  query<T>(query: string, params?: any[], options?: QueryOptions): Promise<Results<T>>;
  sql<T>(sqlStrings: TemplateStringsArray, ...params: any[]): Promise<Results<T>>;
  exec(query: string, options?: QueryOptions): Promise<Array<Results>>;
  rollback(): Promise<void>;
  get closed(): boolean;
}

export declare class DatabaseError extends Error {
  severity: string;
  code: string;
  detail?: string;
  hint?: string;
  position?: string;
  schema?: string;
  table?: string;
  column?: string;
  constraint?: string;
  routine?: string;
  query?: string;
  params?: any[];
}

export declare class PGlite {
  constructor(dataDir?: string, options?: PGliteOptions);
  constructor(options?: PGliteOptions);
  static create(dataDir?: string, options?: PGliteOptions): Promise<PGlite>;

  query<T>(query: string, params?: any[], options?: QueryOptions): Promise<Results<T>>;
  sql<T>(sqlStrings: TemplateStringsArray, ...params: any[]): Promise<Results<T>>;
  exec(query: string, options?: QueryOptions): Promise<Array<Results>>;
  transaction<T>(callback: (tx: Transaction) => Promise<T>): Promise<T>;
  close(): Promise<void>;

  readonly waitReady: Promise<void>;
  readonly ready: boolean;
  readonly closed: boolean;
  /** The database file path, or ":memory:" (Turso extension). */
  readonly path: string;

  [Symbol.asyncDispose](): Promise<void>;
}

export declare const types: {
  BOOL: number;
  BYTEA: number;
  CHAR: number;
  INT8: number;
  INT2: number;
  INT4: number;
  TEXT: number;
  OID: number;
  JSON: number;
  CIDR: number;
  FLOAT4: number;
  FLOAT8: number;
  MACADDR8: number;
  MACADDR: number;
  INET: number;
  BPCHAR: number;
  VARCHAR: number;
  DATE: number;
  TIME: number;
  TIMESTAMP: number;
  TIMESTAMPTZ: number;
  INTERVAL: number;
  NUMERIC: number;
  UUID: number;
  JSONB: number;
};
