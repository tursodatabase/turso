import { chance } from './utils';
import {
  connect
} from '@tursodatabase/react-native';
import {
  expect,
  afterEach,
  beforeEach,
  describe,
  it,
} from '@op-engineering/op-test';

describe('Queries tests', () => {
  let db: any;

  beforeEach(async () => {
    db = connect('queries.sqlite');
  });

  afterEach(() => {
    if (db) {
      // @ts-ignore
      db = null;
    }
  });

  it('executeSync', async () => {
    const res = await db.get('SELECT 1 as value');
    expect(res).toDeepEqual({ value: 1 })
  });
});
