const { Kafka } = require("kafkajs");
/**
 * MANUAL Commit in consumer
 */
const kafka = new Kafka({
  clientId: "NodeConfluentSasl",
  brokers: ["BROKER_URL"],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: "USER_NAME/API_KEY",
    password: "PASSWORD/API_KEY_SECRET",
  },
});

const consumerDetails = {
  groupId: "ConfluentManager",
  topic: "Sample_Topic",
};

const consumerRun = async details => {
  const consumer = kafka.consumer({
    groupId: details.groupId,
    fromBeginning: true,
  });
  await consumer.connect();

  // // It's possible to start from the beginning of the topic
  await consumer.subscribe({
    topic: details.topic,
    fromBeginning: true,
  });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      console.log("message->", {
        key: message.key ? message.key.toString() : "N/A",
        value: message.value ? message.value.toString() : "N/A",
        headers: message.headers,
        topic,
        partition,
        offset: message.offset,
        timestamp: message.timestamp,
      });

      consumer.commitOffsets([
        { topic: topic, partition: partition, offset: message.offset },
      ]);
    },
  });
};

consumerRun(consumerDetails).then(console.log).catch(console.log);

module.exports = {
  consumerRun,
};
