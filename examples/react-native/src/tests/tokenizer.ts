import {isLibsql, open, type DB} from '@tursodatabase/react-native';
import {
  afterEach,
  beforeEach,
  describe,
  it,
  expect,
} from '@op-engineering/op-test';

describe('Tokenizer tests', () => {
  let db: DB;
  beforeEach(async () => {
    db = open({
      name: 'tokenizers.sqlite',
      encryptionKey: 'test',
    });

    if (!isLibsql()) {
      await db.execute('DROP TABLE IF EXISTS tokenizer_table;');
      await db.execute(
        `CREATE VIRTUAL TABLE tokenizer_table USING fts5(content, tokenize = 'wordtokenizer');`,
      );
    }
  });

  afterEach(() => {
    if (db) {
      db.close();
      db.delete();
      // @ts-ignore
      db = null;
    }
  });

  if (!isLibsql()) {
    it('Should match the word split by the tokenizer', async () => {
      await db.execute('INSERT INTO tokenizer_table(content) VALUES (?)', [
        'This is a test document',
      ]);
      const res = await db.execute(
        'SELECT content FROM tokenizer_table WHERE content MATCH ?',
        ['test'],
      );
      expect(res.rows.length).toEqual(1);
      expect(res.rows[0]!.content).toEqual('This is a test document');
    });
  }
});
