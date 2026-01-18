import Chance from 'chance';

export async function sleep(ms: number): Promise<void> {
  return new Promise<void>(resolve => {
    setTimeout(resolve, ms);
  });
}
export const chance = new Chance();
