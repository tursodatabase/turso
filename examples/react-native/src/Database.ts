// import performance from 'react-native-performance';
import Chance from 'chance';
import {open} from '@tursodatabase/react-native';

const chance = new Chance();

const ROWS = 300000;
const DB_NAME = 'largeDB';
const DB_CONFIG = {
  name: DB_NAME,
};

export async function createLargeDB() {
  let largeDb = open(DB_CONFIG);

  largeDb.execute('DROP TABLE IF EXISTS Test;');
  largeDb.execute(
    'CREATE TABLE Test ( id INT PRIMARY KEY, v1 TEXT, v2 TEXT, v3 TEXT, v4 TEXT, v5 TEXT, v6 INT, v7 INT, v8 INT, v9 INT, v10 INT, v11 REAL, v12 REAL, v13 REAL, v14 REAL) STRICT;',
  );

  largeDb.execute('PRAGMA mmap_size=268435456');

  let insertions: [string, any[]][] = [];
  for (let i = 0; i < ROWS; i++) {
    insertions.push([
      'INSERT INTO "Test" (id, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [
        i,
        chance.name(),
        chance.name(),
        chance.name(),
        chance.name(),
        chance.name(),
        chance.integer(),
        chance.integer(),
        chance.integer(),
        chance.integer(),
        chance.integer(),
        chance.floating(),
        chance.floating(),
        chance.floating(),
        chance.floating(),
      ],
    ]);
  }

  await largeDb.executeBatch(insertions);

  largeDb.close();

  console.log('DB created');
}

export async function querySingleRecordOnLargeDB() {
  let largeDb = open(DB_CONFIG);

  await largeDb.execute('SELECT * FROM "Test" LIMIT 1;');
}

export async function queryLargeDB() {
  // let largeDb = open(DB_CONFIG);

  // largeDb.execute('PRAGMA mmap_size=268435456');

  let times: {
    loadFromDb: number[];
    access: number[];
    prepare: number[];
    preparedExecution: number[];
    rawExecution: number[];
  } = {
    loadFromDb: [],
    access: [],
    prepare: [],
    preparedExecution: [],
    rawExecution: [],
  };

  // console.log('Querying DB');

  for (let i = 0; i < 10; i++) {
    // @ts-ignore
    global.gc();

    // let start = performance.now();
    // await largeDb.execute('SELECT * FROM Test;');
    // let end = performance.now();
    // times.loadFromDb.push(end - start);

    // // mmkv.set('largeDB', JSON.stringify(results));
    // // @ts-ignore
    // global.gc();

    // // start = performance.now();
    // // let rawStr = await mmkv.getString('largeDB');
    // // JSON.parse(rawStr!);
    // // end = performance.now();

    // start = performance.now();
    // await largeDb.executeRaw('SELECT * FROM Test;');
    // end = performance.now();
    // times.rawExecution.push(end - start);

    // console.log('MMKV time', (end - start).toFixed(2));

    // @ts-ignore
    // global.gc();

    // performance.mark('accessingStart');
    // const rows = results.rows!._array;
    // for (let i = 0; i < rows.length; i++) {
    //   const v1 = rows[i].v14;
    // }
    // const accessMeasurement = performance.measure(
    //   'accessingEnd',
    //   'accessingStart',
    // );
    // times.access.push(accessMeasurement.duration);

    // // @ts-ignore
    // global.gc();

    // start = performance.now();
    // const statement = largeDb.prepareStatement('SELECT * FROM Test');
    // end = performance.now();
    // times.prepare.push(end - start);

    // // @ts-ignore
    // global.gc();

    // start = performance.now();
    // let results2 = statement.execute();
    // end = performance.now();
    // times.preparedExecution.push(end - start);
  }

  console.log('Querying DB done');

  return times;
}
