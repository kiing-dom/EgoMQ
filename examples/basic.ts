import { Queue } from '../src/queue';

const queue = new Queue();

queue.register("hello", async(payload: {name: string}) => {
    console.log(`hello ${payload.name}!`);
})

await queue.enqueue("hello", { name: "dom"});

console.log("jobs remaining:", queue.getJobs())
queue.getJobs();

await queue.start();

console.log("jobs remaining:", queue.getJobs())
queue.getJobs();