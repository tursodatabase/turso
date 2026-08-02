import { DatabaseError, TimeoutError } from './error.js';

export interface Value {
  type: 'null' | 'integer' | 'float' | 'text' | 'blob';
  value?: string | number;
  base64?: string;
}

export interface Column {
  name: string;
  decltype: string;
}

export interface ExecuteResult {
  cols: Column[];
  rows: Value[][];
  affected_row_count: number;
  last_insert_rowid?: string | number;
}

export interface NamedArg {
  name: string;
  value: Value;
}

export interface ExecuteRequest {
  type: 'execute';
  stmt: {
    sql: string;
    args: Value[];
    named_args: NamedArg[];
    want_rows: boolean;
  };
}

export type BatchCondition =
  | { type: 'ok'; step: number }
  | { type: 'error'; step: number }
  | { type: 'not'; cond: BatchCondition }
  | { type: 'and'; conds: BatchCondition[] }
  | { type: 'or'; conds: BatchCondition[] }
  | { type: 'is_autocommit' };

export interface BatchStep {
  stmt: {
    sql: string;
    args: Value[];
    named_args?: NamedArg[];
    want_rows: boolean;
  };
  condition?: BatchCondition;
}

export interface BatchRequest {
  type: 'batch';
  batch: {
    steps: BatchStep[];
  };
}

export interface SequenceRequest {
  type: 'sequence';
  sql: string;
}

export interface CloseRequest {
  type: 'close';
}

export interface DescribeRequest {
  type: 'describe';
  sql: string;
}

export interface GetAutocommitRequest {
  type: 'get_autocommit';
}

export interface DescribeResult {
  params: Array<{ name?: string }>;
  cols: Column[];
  is_explain: boolean;
  is_readonly: boolean;
}

export interface PipelineRequest {
  baton: string | null;
  requests: (ExecuteRequest | BatchRequest | SequenceRequest | CloseRequest | DescribeRequest | GetAutocommitRequest)[];
}

export interface PipelineResponse {
  baton: string | null;
  base_url: string | null;
  results: Array<{
    type: 'ok' | 'error';
    response?: {
      type: 'execute' | 'batch' | 'sequence' | 'close' | 'describe' | 'get_autocommit';
      result?: ExecuteResult | DescribeResult;
      is_autocommit?: boolean;
    };
    error?: {
      message: string;
      code: string;
    };
  }>;
}

function toBase64(uint8: Uint8Array): string {
  return Buffer.from(uint8.buffer, uint8.byteOffset, uint8.byteLength).toString('base64');
}

export function encodeValue(value: any): Value {
  if (value === null || value === undefined) {
    return { type: 'null' };
  }
  
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new Error("Only finite numbers (not Infinity or NaN) can be passed as arguments");
    }
    if (Number.isSafeInteger(value)) {
      return { type: 'integer', value: value.toString() };
    }
    return { type: 'float', value };
  }
  
  if (typeof value === 'bigint') {
    return { type: 'integer', value: value.toString() };
  }
  
  if (typeof value === 'boolean') {
    return { type: 'integer', value: value ? '1' : '0' };
  }
  
  if (typeof value === 'string') {
    return { type: 'text', value };
  }
  
  if (value instanceof ArrayBuffer) {
    return { type: 'blob', base64: toBase64(new Uint8Array(value)) };
  }

  if (value instanceof Uint8Array) {
    return { type: 'blob', base64: toBase64(value) };
  }
  
  return { type: 'text', value: String(value) };
}

export function decodeValue(value: Value, safeIntegers: boolean = false): any {
  switch (value.type) {
    case 'null':
      return null;
    case 'integer':
      if (safeIntegers) {
        return BigInt(value.value as string);
      }
      return parseInt(value.value as string, 10);
    case 'float':
      return value.value as number;
    case 'text':
      return value.value as string;
    case 'blob':
      if (value.base64 !== undefined && value.base64 !== null) {
        let b64 = value.base64;
        while (b64.length % 4 !== 0) {
          b64 += '=';
        }
        const binaryString = atob(b64);
        const bytes = new Uint8Array(binaryString.length);
        for (let i = 0; i < binaryString.length; i++) {
          bytes[i] = binaryString.charCodeAt(i);
        }
        return Buffer.from(bytes);
      }
      return Buffer.alloc(0);
    default:
      return null;
  }
}

export interface CursorRequest {
  baton: string | null;
  batch: {
    steps: BatchStep[];
  };
}

export interface CursorResponse {
  baton: string | null;
  base_url: string | null;
}

export interface CursorEntry {
  type: 'step_begin' | 'step_end' | 'step_error' | 'row' | 'error';
  step?: number;
  cols?: Column[];
  row?: Value[];
  affected_row_count?: number;
  last_insert_rowid?: string | number;
  error?: {
    message: string;
    code: string;
  };
}

/** HTTP header key for the encryption key */
export const ENCRYPTION_KEY_HEADER = 'x-turso-encryption-key';

/**
 * Per-request HTTP context: where to send the request and which headers to
 * attach. Built by the Session from its config and current base URL.
 */
export interface HttpContext {
  /** Base URL requests are sent to. */
  url: string;
  /** Authentication token, sent as `Authorization: Bearer <token>`. */
  authToken?: string;
  /** Encryption key for the remote database, sent as `x-turso-encryption-key`. */
  remoteEncryptionKey?: string;
  /**
   * Extra HTTP headers attached to the request. Applied after the standard
   * headers, so they can override e.g. `Authorization`. Passing the `Host`
   * key (case-insensitive) throws — fetch forbids setting it.
   */
  requestHeaders?: Record<string, string>;
}

function buildHeaders(ctx: HttpContext): Record<string, string> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  };
  if (ctx.authToken) {
    headers['Authorization'] = `Bearer ${ctx.authToken}`;
  }
  if (ctx.remoteEncryptionKey) {
    headers[ENCRYPTION_KEY_HEADER] = ctx.remoteEncryptionKey;
  }
  for (const [name, value] of Object.entries(ctx.requestHeaders ?? {})) {
    // `Host` is a forbidden fetch header and would be silently dropped —
    // throw instead so the caller learns the override never takes effect.
    if (name.toLowerCase() === 'host') {
      throw new DatabaseError("overwriting the 'Host' header is not supported");
    }
    headers[name] = value;
  }
  return headers;
}

function buildFetchOptions(ctx: HttpContext, body: string, signal?: AbortSignal): RequestInit {
  return {
    method: 'POST',
    headers: buildHeaders(ctx),
    body,
    signal,
  };
}

/** Per-query options. Override the session-level defaults for a single call. */
export interface QueryOptions {
  /** Per-query timeout in milliseconds. Overrides defaultQueryTimeout for this call. */
  queryTimeout?: number;
  /**
   * Extra HTTP headers attached to this request only. Applied after the
   * standard headers and any session-level `requestHeaders`, so they can
   * override both. Passing the `Host` key (case-insensitive) throws —
   * fetch forbids setting it.
   */
  requestHeaders?: Record<string, string>;
}

function wrapAbortError(error: unknown): never {
  if (error instanceof Error && (error.name === 'AbortError' || error.name === 'TimeoutError')) {
    throw new TimeoutError('Query timed out');
  }
  throw error;
}

export async function executeCursor(
  ctx: HttpContext,
  request: CursorRequest,
  signal?: AbortSignal
): Promise<{ response: CursorResponse; entries: AsyncGenerator<CursorEntry> }> {
  let response: Response;
  try {
    response = await fetch(`${ctx.url}/v3/cursor`, buildFetchOptions(ctx, JSON.stringify(request), signal));
  } catch (error) {
    wrapAbortError(error);
  }

  if (!response.ok) {
    let errorMessage = `HTTP error! status: ${response.status}`;
    try {
      const errorBody = await response.text();
      const errorData = JSON.parse(errorBody);
      if (errorData.message) {
        errorMessage = errorData.message;
      }
    } catch {
      // If we can't parse the error body, use the default HTTP error message
    }
    throw new DatabaseError(errorMessage);
  }

  const reader = response.body?.getReader();
  if (!reader) {
    throw new DatabaseError('No response body');
  }

  const decoder = new TextDecoder();
  let buffer = '';
  let cursorResponse: CursorResponse | undefined;

  // First, read until we get the cursor response (first line)
  try {
    while (!cursorResponse) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });

      const newlineIndex = buffer.indexOf('\n');
      if (newlineIndex !== -1) {
        const line = buffer.slice(0, newlineIndex).trim();
        buffer = buffer.slice(newlineIndex + 1);

        if (line) {
          cursorResponse = JSON.parse(line);
          break;
        }
      }
    }
  } catch (error) {
    reader.releaseLock();
    wrapAbortError(error);
  }

  if (!cursorResponse) {
    reader.releaseLock();
    throw new DatabaseError('No cursor response received');
  }

  async function* parseEntries(): AsyncGenerator<CursorEntry> {
    try {
      // Process any remaining data in the buffer
      let newlineIndex;
      while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, newlineIndex).trim();
        buffer = buffer.slice(newlineIndex + 1);

        if (line) {
          yield JSON.parse(line) as CursorEntry;
        }
      }

      // Continue reading from the stream
      while (true) {
        let readResult: ReadableStreamReadResult<Uint8Array>;
        try {
          readResult = await reader!.read();
        } catch (error) {
          wrapAbortError(error);
        }
        if (readResult.done) break;

        buffer += decoder.decode(readResult.value, { stream: true });

        while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
          const line = buffer.slice(0, newlineIndex).trim();
          buffer = buffer.slice(newlineIndex + 1);

          if (line) {
            yield JSON.parse(line) as CursorEntry;
          }
        }
      }

      // Process any remaining data in the buffer
      if (buffer.trim()) {
        yield JSON.parse(buffer.trim()) as CursorEntry;
      }
    } finally {
      reader!.releaseLock();
    }
  }

  return { response: cursorResponse, entries: parseEntries() };
}

export async function executePipeline(
  ctx: HttpContext,
  request: PipelineRequest,
  signal?: AbortSignal
): Promise<PipelineResponse> {
  let response: Response;
  try {
    response = await fetch(`${ctx.url}/v3/pipeline`, buildFetchOptions(ctx, JSON.stringify(request), signal));
  } catch (error) {
    wrapAbortError(error);
  }

  if (!response.ok) {
    throw new DatabaseError(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}
