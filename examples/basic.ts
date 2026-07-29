import { Queue } from '../src/queue';

const queue = new Queue();

queue.register("hello", async(payload: {name: string}) => {
    console.log(`hello ${payload.name}!`);
})

queue.enqueue({
    type: "hello",
    payload: {
        name: "dom",
    },
});

queue.start();