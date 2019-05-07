import fetch from 'node-fetch';
import delay from 'delay';

jest.setTimeout(40000);

describe('basic flow', () => {
    it('services are alive', async () => {
        try {
            await delay(30000);
            await fetch('http://localhost:2000/isAlive'); // consumer
            await fetch('http://localhost:2500/isAlive'); // producer
        } catch (e) {
            console.log(e.text ? await e.text() : e);
            throw e;
        }
    });

    describe('logic', () => {});
});
