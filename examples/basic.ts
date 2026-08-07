import { Queue } from '../src/queue';

const TEST_DB = "./db/example.db";
const schemaPath = "./schema.sql"

const queue = new Queue(TEST_DB, schemaPath);

queue.register("hello", async(payload: {name: string}) => {
    console.log(`hello ${payload.name}!`);
})

await queue.enqueue("hello", { name: "dom"});

console.log("jobs remaining:", queue.getJobs())
queue.getJobs();

await queue.start();

console.log("jobs remaining:", queue.getJobs())
queue.getJobs();