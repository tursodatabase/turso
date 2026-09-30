import { expect, test } from 'vitest'
import { maybePromise } from './promise.js'

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