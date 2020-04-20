import delay from 'delay';
import fetch from 'node-fetch';
import Server from 'simple-fake-server-server-client';
import {range} from 'lodash';

import readinessCheck from '../readinessCheck';
import * as uuid from 'uuid';

jest.setTimeout(180000);

const fakeHttpServer = new Server({
    baseUrl: `http://localhost`,
    port: 3000,
});

describe('tests', () => {
    beforeAll(async () => {
        await expect(readinessCheck()).resolves.toBeTruthy();
    });

    beforeEach(async () => {
        await fakeHttpServer.clear();
    });

    it('liveliness', async () => {
        await delay(10000);
        const producer = await fetch('http://localhost:6000/isAlive');
        if (!producer.ok) {
            fail('producer not alive');
        }

        // const consumer = await fetch('http://localhost:4000/isAlive');
        // if (!consumer.ok) {
        //     fail('consumer not alive');
        // }
    });

    it('should produce and consume', async () => {
        const callId = await mockHttpTarget();

        await produce('http://localhost:6000/produce', [{topic: 'test', key: uuid(), value: {data: 'test'}}]);
        await produce('http://localhost:6000/produce', [
            {topic: 'another-test', key: uuid(), value: {data: 'another-test'}},
        ]);

        await delay(5000);

        const {hasBeenMade, madeCalls} = await fakeHttpServer.getCall(callId);
        expect(hasBeenMade).toBeTruthy();
        expect(madeCalls.length).toBe(2);
        expect(madeCalls[0].headers['x-record-topic']).toBe('test');
        expect(madeCalls[1].headers['x-record-topic']).toBe('another-test');
    });

    it('should support backpressure', async () => {
        const callId = await mockHttpTarget();

        const recordsCount = 1000;
        const records = range(recordsCount).map((i: number) => ({topic: 'test', key: uuid(), value: {data: i}}));

        await produce('http://localhost:6000/produce', records);
        await delay(recordsCount * 10);

        const {madeCalls} = await fakeHttpServer.getCall(callId);
        expect(madeCalls.length).toBe(recordsCount);
    });

    it('producer request validation', async () => {
        const method = 'post';
        const producerUrl = 'http://localhost:6000/produce';
        const headers = {'Content-Type': 'application/json'};
        let response;

        response = await fetch(producerUrl, {
            method,
            body: JSON.stringify([{key: 'key', value: {data: 1}}]),
            headers,
        });
        expect(response.status).toBe(400);
        expect(await response.text()).toBe('topic is missing');

        response = await fetch(producerUrl, {
            method,
            body: JSON.stringify([{topic: 'test', value: {data: 1}}]),
            headers,
        });
        expect(response.status).toBe(400);
        expect(await response.text()).toBe('key is missing');

        response = await fetch(producerUrl, {
            method,
            body: JSON.stringify([{topic: 'test', key: 'key'}]),
            headers,
        });
        expect(response.status).toBe(400);
        expect(await response.text()).toBe('value is missing');
    });
});

const produce = (url: string, batch: any[]) =>
    fetch(url, {
        method: 'post',
        body: JSON.stringify(batch),
        headers: {'Content-Type': 'application/json'},
    });

const mockHttpTarget = () =>
    fakeHttpServer.mock({
        method: 'post',
        url: '/consume',
    });
