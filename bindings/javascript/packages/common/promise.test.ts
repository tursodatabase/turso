import { expect, test } from 'vitest'
import { maybePromise, maybeValue, Statement } from './promise.js'

test('drizzle-orm', async () => {
    const lazy = maybePromise(() => fetch('http://google.com'));
    let status, headers;
    //@ts-ignore
    lazy.apply(x => { status = x.status; })
    //@ts-ignore
    lazy.apply(x => { headers = x.headers; })
    let response = await lazy.resolve();
    expect(response).not.toBeNull();
    expect(status).toBe(200);
    expect(headers).not.toBeNull();
})

function nativeStatement() {
    const bindings: Array<[number, unknown]> = [];
    return {
        bindings,
        bindAt(index: number, value: unknown) {
            bindings.push([index, value]);
        },
    };
}

test('Statement.bind applies values to an eagerly available native statement', () => {
    const native = nativeStatement();
    const statement = new Statement(maybeValue(native as any), {} as any, {} as any, async () => {});

    expect(statement.bind('second')).toBe(statement);
    expect(native.bindings).toEqual([[1, 'second']]);
});

test('Statement.bind queues values for a lazily prepared native statement', async () => {
    const native = nativeStatement();
    const lazy = maybePromise(async () => native as any);
    const statement = new Statement(lazy, {} as any, {} as any, async () => {});

    expect(statement.bind('second')).toBe(statement);
    await lazy.resolve();
    expect(native.bindings).toEqual([[1, 'second']]);
});
