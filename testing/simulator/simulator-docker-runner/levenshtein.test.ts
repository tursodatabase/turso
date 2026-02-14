import { test, expect } from "bun:test";
import { levenshtein } from "./levenshtein";

test("levenshtein distance between empty strings is 0", () => {
  expect(levenshtein("", "")).toBe(0);
});

test("levenshtein distance between completely different same length strings is their length", () => {
  expect(levenshtein("abcde", "fghij")).toBe(5);
  expect(levenshtein("fghij", "abcde")).toBe(5);
});

test("levenshtein distance between strings with one edit is 1", () => {
  expect(levenshtein("hello", "hallo")).toBe(1);
  expect(levenshtein("hallo", "hello")).toBe(1);
});

test("levenshtein distance between otherwise identical strings with length difference is their length difference", () => {
  expect(levenshtein("hello", "hello world")).toBe(6);
  expect(levenshtein("hello world", "hello")).toBe(6);
});

test("levenshtein distance between strings with multiple edits is the sum of the edits", () => {
  expect(levenshtein("hello", "hallu")).toBe(2);
  expect(levenshtein("hallu", "hello")).toBe(2);
});