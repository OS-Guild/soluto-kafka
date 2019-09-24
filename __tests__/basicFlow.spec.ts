import delay from 'delay';
import * as got from 'got';

jest.setTimeout(40000);

describe('basic flow', () => {
    it('services are alive', async () => {
        try {
            await delay(30000);
            await got('http://localhost:2000/isAlive'); // consumer
            await got('http://localhost:2500/isAlive'); // producer
        } catch (e) {
            console.log(e);
            throw e;
        }
    });

    describe('logic', () => {
        beforeAll(() => got.post('http://localhost:3000/fake_server_admin/clear'));

        it('should produce and consume', async () => {
            const {
                body: {callId},
            } = await got.post('http://localhost:3000/fake_server_admin/calls', {
                json: true,
                body: {
                    method: 'post',
                    url: '/target',
                    statusCode: 200,
                },
            });
            await got.post('http://localhost:2500/produce', {json: true, body: [{key: 'key', message: {data: 1}}]});

            await delay(500);

            const {
                body: {hasBeenMade},
            } = await got(`http://localhost:3000/fake_server_admin/calls?callId=${callId}`, {json: true});

            expect(hasBeenMade).toBeTruthy();
        });
    });
});
