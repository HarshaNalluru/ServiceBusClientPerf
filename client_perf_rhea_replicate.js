const { Connection, delay } = require("rhea-promise");
const moment = require('moment');

const host = ""; // <your-namespace>.servicebus.windows.net
const username = ""; // SharedAccessKeyName (usually  "RootManageSharedAccessKey")
const password = ""; // SharedAccessKey
const port = 5671;
const senderAddress = ""; // Queue name

const _payload = Buffer.alloc(1024);
const _start = moment();

let _messages = 0;

async function main() {

  const maxInflight = process.argv.length > 2 ? parseInt(process.argv[2]) : 1;
  const messages = process.argv.length > 3 ? parseInt(process.argv[3]) : 10;
  log(`Maximum inflight messages: ${maxInflight}`);
  log(`Total messages: ${messages}`);

  let writeResultsPromise = WriteResults(messages);
  
  await RunTest(maxInflight, messages);

  await writeResultsPromise;
};

async function RunTest(maxInflight, messages) {
  
  const connection = new Connection({
      transport: "tls",
      host: host,
      hostname: host,
      username: username,
      password: password,
      port: port,
      reconnect: false
  });
  await connection.open();

  const sender = await connection.createSender({
    name: "sender-1",
    target: {
        address: senderAddress
      }
  });    
  let promises = [];

  for (let i = 0; i < maxInflight; i++ ) {
    let promise = ExecuteSendsAsync(sender, messages);
    promises[i] = promise;
  }

  await Promise.all(promises);

  await connection.close();
}

async function ExecuteSendsAsync(sender, messages) {
  while (++_messages <= messages) {
    await sender.send({ body: _payload });
  }

  // Undo last increment, since a message was never sent on the final loop iteration
  _messages--;
}

async function WriteResults(messages) {
  let lastMessages = 0;
  let lastElapsed = 0;
  let maxMessages = 0;
  let maxElapsed = Number.MAX_SAFE_INTEGER;

  do {
    await delay(1000);

    let sentMessages = _messages;
    let currentMessages = sentMessages - lastMessages;
    lastMessages = sentMessages;

    let elapsed = moment().diff(_start);
    let currentElapsed = elapsed - lastElapsed;
    lastElapsed = elapsed;

    if ((currentMessages / currentElapsed) > (maxMessages / maxElapsed)) {
      maxMessages = currentMessages;
      maxElapsed = currentElapsed;
    }

    WriteResult(sentMessages, elapsed, currentMessages, currentElapsed, maxMessages, maxElapsed);
  }
  while (_messages < messages);
}

function WriteResult(totalMessages, totalElapsed,
  currentMessages, currentElapsed,
  maxMessages, maxElapsed) {
  log(`\tTot Msg\t${totalMessages}` +
      `\tCur MPS\t${Math.round((currentMessages * 1000) / currentElapsed)}` +
      `\tAvg MPS\t${Math.round((totalMessages * 1000) / totalElapsed)}` +
      `\tMax MPS\t${Math.round((maxMessages * 1000) / maxElapsed)}`
      );
}

function log(message) {
  console.log(`[${moment().format('hh:mm:ss.SSS')}] ${message}`);
}

main().catch(err => {
  log(`Error occurred: ${err}`);
});