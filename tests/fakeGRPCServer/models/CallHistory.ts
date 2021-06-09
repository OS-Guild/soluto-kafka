export type Call = {
    headers: any;
    payload: any;
};

export default class CallHistory {
    calls: Call[];

    constructor() {
        this.calls = [];
    }

    get() {
        return this.calls;
    }

    push(call: Call) {
        this.calls.push(call);
    }

    clear() {
        this.calls = [];
    }
}
