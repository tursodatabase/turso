import { expect, test } from 'vitest';
import { bindParams } from './bind.js';

function statement(names: (string | null)[]) {
  const values = new Map<number, unknown>();
  return {
    values,
    parameterCount: () => names.length,
    parameterName: (index: number) => names[index - 1],
    bindAt: (index: number, value: unknown) => values.set(index, value),
  };
}

test('omitted positional parameters cannot reuse a previous binding', () => {
  const stmt = statement([null, null]);
  bindParams(stmt, ['first', 'second']);
  expect(() => bindParams(stmt, [])).toThrow('Too few parameter values');
  expect(() => bindParams(stmt, ['third'])).toThrow('Too few parameter values');
  expect([...stmt.values.values()]).toEqual(['first', 'second']);
  bindParams(stmt, [['third', null]]);
  expect([...stmt.values.values()]).toEqual(['third', null]);
});

test('omitted named parameters cannot reuse a previous binding', () => {
  const stmt = statement([':key', ':value']);
  bindParams(stmt, [{ key: 'first', value: 'second' }]);
  expect(() => bindParams(stmt, [{ key: 'third' }])).toThrow('Too few parameter values');
  expect(() => bindParams(stmt, [{ zz: 'third' }])).toThrow('Too few parameter values');
  expect([...stmt.values.values()]).toEqual(['first', 'second']);
  bindParams(stmt, [{ key: null, value: 'third' }]);
  expect([...stmt.values.values()]).toEqual([null, 'third']);
});

test('permanently bound values remain available without execution arguments', () => {
  const stmt = statement([null]);
  bindParams(stmt, ['first']);
  bindParams(stmt, [], true);
  expect(stmt.values.get(1)).toBe('first');
});

test('statements without placeholders accept no arguments', () => {
  bindParams(statement([]), []);
});
