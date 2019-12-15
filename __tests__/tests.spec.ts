import delay from 'delay';
import * as got from 'got';
import fetch from 'node-fetch';

jest.setTimeout(60000);

describe('tests', () => {
    beforeAll(async () => {
        await delay(30000)
        beforeAll(() => got.post('http://localhost:3000/fake_server_admin/clear'));

    })
    it('services are alive', async () => {
        let attempts = 3;
        while (attempts > 0) {
            try {
                const responseConsumer = await fetch('http://localhost:4000/isAlive');
                const responseProducer = await fetch('http://localhost:6000/isAlive');
                if (responseConsumer.ok && responseProducer.ok) {
                    return;
                }
                attempts--;

            } catch (e) {
                console.log("not alive", e.message)
                attempts--;
            }
            await delay(10000)
        }
        fail();
    });

    it('should produce and consume', async () => {
        const {
            body: {callId},
        } = await got.post('http://localhost:3000/fake_server_admin/calls', {
            json: true,
            body: {
                method: 'post',
                url: '/',
                statusCode: 200,
            },
        });
        await fetch('http://localhost:6000/produce', {
            method: 'post',
            body: JSON.stringify([{key: 'key', message: {data: 1}}]),
            headers: {'Content-Type': 'application/json'},
        });

        await delay(5000);

        const {
            body: {hasBeenMade},
        } = await got(`http://localhost:3000/fake_server_admin/calls?callId=${callId}`, {json: true});

        expect(hasBeenMade).toBeTruthy();
    });
});
