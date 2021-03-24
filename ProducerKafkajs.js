
const { Kafka } = require("kafkajs");

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

const producer = kafka.producer({ allowAutoTopicCreation: false });

const postMessage = async message => {
  try {
    const givenTopicName = message.topicName;
    const givenApplicationName = message.applicationName;
    const actualToPublishMessage = message.data;
    const bufferData = Buffer.from(JSON.stringify(actualToPublishMessage));

    await producer.connect();

    const data = await producer.send({
      topic: givenTopicName,
      messages: [
        {
          key: `${Date.now()}`,
          value: bufferData,
        },
      ],
      acks: -1,
      timeout: 20000,
    });

    /**
     * Example data result
     * 
       * [
     * {
     * topicName: 'Sample_topic',
     * partition: 0,
     * errorCode: 0,
     * baseOffset: '2',
     * logAppendTime: '-1',
     * logStartOffset: '0'
     * }
     * ]
    */

    console.log("~~ Produced ~~", data);

    return {
      status: 201,
      response: data,
    };
  } catch (error) {
    console.log("Error occurred:", error);
    return {
      status: 500,
      response: error,
    };
  }
};

postMessage({
  topicName: "Sample_topic",
  applicationName: "xlosConfluentTest",
  msgDateTime: new Date(),
  data: {
    msg: "This is a test message for confluent demo",
  },
});

module.exports = {
  postMessage,
};
