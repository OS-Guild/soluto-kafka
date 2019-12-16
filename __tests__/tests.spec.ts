import delay from 'delay';
import * as got from 'got';
import fetch from 'node-fetch';

jest.setTimeout(60000);

describe('tests', () => {
    beforeAll(async () => {
        await delay(30000)
        await fetch('http://localhost:4771/clear');
        await got.post('http://localhost:3000/fake_server_admin/clear');
    });

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
                console.log('not alive', e.message);
                attempts--;
            }
            await delay(10000);
        }
        fail();
    });

    it('should produce and consume', async () => {
        await fetch('http://localhost:4771/add', {
            method: 'post',
            headers: {'content-type': 'application-json'},
            body: JSON.stringify({
                service: 'CallTarget',
                method: 'callTarget',
                input: {matches: {recordTimestamp: '1.576488519765e+12', msgJson: '{"data":1}',}},
                output: {
                    data: {
                        message: 'assertion',
                    },
                },
            }),
        });

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
