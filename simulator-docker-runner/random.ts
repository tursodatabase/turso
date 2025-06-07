export function randomSeed() {
  const high32 = Math.floor(Math.random() * Math.pow(2, 32));
  const low32 = Math.floor(Math.random() * Math.pow(2, 32));
  return ((BigInt(high32) << 32n) | BigInt(low32)).toString();
}