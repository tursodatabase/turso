import { DatabaseError } from './error.js';

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
  last_insert_rowid?: string;
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

export interface BatchStep {
  stmt: {
    sql: string;
    args: Value[];
    named_args?: NamedArg[];
    want_rows: boolean;
  };
  condition?: {
    type: 'ok';
    step: number;
  };
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

export interface DescribeResult {
  params: Array<{ name?: string }>;
  cols: Column[];
  is_explain: boolean;
  is_readonly: boolean;
}

export interface PipelineRequest {
  baton: string | null;
  requests: (ExecuteRequest | BatchRequest | SequenceRequest | CloseRequest | DescribeRequest)[];
}

export interface PipelineResponse {
  baton: string | null;
  base_url: string | null;
  results: Array<{
    type: 'ok' | 'error';
    response?: {
      type: 'execute' | 'batch' | 'sequence' | 'close' | 'describe';
      result?: ExecuteResult | DescribeResult;
    };
    error?: {
      message: string;
      code: string;
    };
  }>;
}

export function encodeValue(value: any): Value {
  if (value === null || value === undefined) {
    return { type: 'null' };
  }
  
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new Error("Only finite numbers (not Infinity or NaN) can be passed as arguments");
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
  
  if (value instanceof ArrayBuffer || value instanceof Uint8Array) {
    const base64 = btoa(String.fromCharCode(...new Uint8Array(value)));
    return { type: 'blob', base64 };
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
      if (value.base64) {
        const binaryString = atob(value.base64);
        const bytes = new Uint8Array(binaryString.length);
        for (let i = 0; i < binaryString.length; i++) {
          bytes[i] = binaryString.charCodeAt(i);
        }
        return Buffer.from(bytes);
      }
      return null;
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
  last_insert_rowid?: string;
  error?: {
    message: string;
    code: string;
  };
}

/** HTTP header key for the encryption key */
export const ENCRYPTION_KEY_HEADER = 'x-turso-encryption-key';

export async function executeCursor(
  url: string,
  authToken: string,
  request: CursorRequest,
  remoteEncryptionKey?: string
): Promise<{ response: CursorResponse; entries: AsyncGenerator<CursorEntry> }> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${authToken}`,
  };
  if (remoteEncryptionKey) {
    headers[ENCRYPTION_KEY_HEADER] = remoteEncryptionKey;
  }

  const response = await fetch(`${url}/v3/cursor`, {
    method: 'POST',
    headers,
    body: JSON.stringify(request),
  });

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

  if (!cursorResponse) {
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
        const { done, value } = await reader!.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        
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
  url: string,
  authToken: string,
  request: PipelineRequest,
  remoteEncryptionKey?: string
): Promise<PipelineResponse> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${authToken}`,
  };
  if (remoteEncryptionKey) {
    headers[ENCRYPTION_KEY_HEADER] = remoteEncryptionKey;
  }

  const response = await fetch(`${url}/v3/pipeline`, {
    method: 'POST',
    headers,
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    throw new DatabaseError(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}
