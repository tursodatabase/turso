// PostgreSQL type OIDs and default value conversion, PGlite-compatible.
//
// Result values are converted from their raw form to JS by result-column
// type OID, reproducing the mapping documented in postgres/PGLITE.md.
// Custom parsers/serializers (keyed by OID) receive and produce the
// PostgreSQL *text* representation, exactly as in PGlite.

export const BOOL = 16;
export const BYTEA = 17;
export const CHAR = 18;
export const INT8 = 20;
export const INT2 = 21;
export const INT4 = 23;
export const TEXT = 25;
export const OID = 26;
export const JSON_T = 114;
export const CIDR = 650;
export const FLOAT4 = 700;
export const FLOAT8 = 701;
export const MACADDR8 = 774;
export const MACADDR = 829;
export const INET = 869;
export const BPCHAR = 1042;
export const VARCHAR = 1043;
export const DATE = 1082;
export const TIME = 1083;
export const TIMESTAMP = 1114;
export const TIMESTAMPTZ = 1184;
export const INTERVAL = 1186;
export const NUMERIC = 1700;
export const UUID = 2950;
export const JSONB = 3802;

export { JSON_T as JSON };

// Array type OID -> element type OID.
const ARRAY_ELEMENT_OID = {
  199: JSON_T,
  651: CIDR,
  775: MACADDR8,
  1000: BOOL,
  1001: BYTEA,
  1005: INT2,
  1007: INT4,
  1009: TEXT,
  1015: VARCHAR,
  1016: INT8,
  1021: FLOAT4,
  1022: FLOAT8,
  1040: MACADDR,
  1041: INET,
  1115: TIMESTAMP,
  1182: DATE,
  1183: TIME,
  1185: TIMESTAMPTZ,
  1231: NUMERIC,
  2951: UUID,
  3807: JSONB,
};

export const arrayElementOid = (oid) => ARRAY_ELEMENT_OID[oid];

const hex = (buf) => {
  let out = "\\x";
  for (const byte of buf) out += byte.toString(16).padStart(2, "0");
  return out;
};

const decodeHex = (text) => {
  const out = new Uint8Array((text.length - 2) / 2);
  for (let i = 2; i < text.length; i += 2) {
    out[(i - 2) / 2] = parseInt(text.slice(i, i + 2), 16);
  }
  return out;
};

// The PostgreSQL text representation of a raw native value, fed to custom
// parsers and used as the fallback for unknown OIDs.
export const textForm = (raw) => {
  if (raw instanceof Uint8Array) return hex(raw);
  return String(raw);
};

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE = BigInt(-Number.MAX_SAFE_INTEGER);

// Parse a PostgreSQL array literal (e.g. `{1,2,3}` or `{"a","b",NULL}`)
// into a JS array, converting each element with `parseElement`.
export const parseArray = (text, parseElement) => {
  const result = [];
  let i = 1; // skip '{'
  if (text[i] === "}") return result;
  while (i < text.length) {
    if (text[i] === "{") {
      // nested array: find the matching brace
      let depth = 0;
      const start = i;
      while (i < text.length) {
        if (text[i] === "{") depth++;
        else if (text[i] === "}") depth--;
        i++;
        if (depth === 0) break;
      }
      result.push(parseArray(text.slice(start, i), parseElement));
    } else if (text[i] === '"') {
      let value = "";
      i++;
      while (text[i] !== '"') {
        if (text[i] === "\\") i++;
        value += text[i];
        i++;
      }
      i++;
      result.push(parseElement(value));
    } else {
      const start = i;
      while (text[i] !== "," && text[i] !== "}") i++;
      const value = text.slice(start, i);
      result.push(value === "NULL" ? null : parseElement(value));
    }
    if (text[i] === ",") {
      i++;
    } else if (text[i] === "}") {
      break;
    }
  }
  return result;
};

// Serialize a JS array to a PostgreSQL array literal.
export const serializeArray = (values, serializeElement) => {
  const parts = values.map((v) => {
    if (v === null || v === undefined) return "NULL";
    if (Array.isArray(v)) return serializeArray(v, serializeElement);
    const s = serializeElement(v);
    if (/[{},"\\\s]/.test(s) || s === "" || s === "NULL") {
      return '"' + s.replace(/\\/g, "\\\\").replace(/"/g, '\\"') + '"';
    }
    return s;
  });
  return "{" + parts.join(",") + "}";
};

// Convert a raw native value (null | bigint | number | string | Uint8Array)
// to the PGlite-compatible JS value for the given result-column OID.
export const parseValue = (raw, oid, parsers) => {
  if (raw === null || raw === undefined) return null;
  const custom = parsers && parsers[oid];
  if (custom) return custom(textForm(raw));
  switch (oid) {
    case BOOL:
      if (typeof raw === "string") return raw === "t" || raw === "true";
      return Number(raw) !== 0;
    case INT2:
    case INT4:
    case OID:
      return Number(raw);
    case INT8:
      if (typeof raw === "bigint") {
        return raw <= MAX_SAFE && raw >= MIN_SAFE ? Number(raw) : raw;
      }
      return Number(raw);
    case FLOAT4:
    case FLOAT8:
      return Number(raw);
    case NUMERIC:
      return String(raw);
    case JSON_T:
    case JSONB:
      return JSON.parse(String(raw));
    case BYTEA:
      if (raw instanceof Uint8Array) return Uint8Array.from(raw);
      if (typeof raw === "string" && raw.startsWith("\\x")) return decodeHex(raw);
      return Uint8Array.from([]);
    case DATE:
    case TIMESTAMP:
    case TIMESTAMPTZ:
      return new Date(String(raw));
    default: {
      const elementOid = arrayElementOid(oid);
      if (elementOid !== undefined) {
        return parseArray(String(raw), (v) => parseValue(v, elementOid, parsers));
      }
      return textForm(raw);
    }
  }
};

// Convert a JS parameter value to what the native layer binds. Custom
// serializers (keyed by the explicit paramTypes OID) produce text.
export const serializeValue = (value, oid, serializers) => {
  if (value === null || value === undefined) return null;
  const custom = oid !== undefined && serializers && serializers[oid];
  if (custom) return custom(value);
  if (value instanceof Date) return value.toISOString();
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) {
    return serializeArray(value, (v) =>
      typeof v === "object" ? JSON.stringify(v) : String(v),
    );
  }
  if (typeof value === "object") return JSON.stringify(value);
  return value; // boolean | number | bigint | string pass through natively
};

export const types = {
  BOOL,
  BYTEA,
  CHAR,
  INT8,
  INT2,
  INT4,
  TEXT,
  OID,
  JSON: JSON_T,
  CIDR,
  FLOAT4,
  FLOAT8,
  MACADDR8,
  MACADDR,
  INET,
  BPCHAR,
  VARCHAR,
  DATE,
  TIME,
  TIMESTAMP,
  TIMESTAMPTZ,
  INTERVAL,
  NUMERIC,
  UUID,
  JSONB,
};
