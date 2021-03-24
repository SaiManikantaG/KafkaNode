import { kafkaEvents } from "../../common/constants";
import * as _ from "lodash";
import logger from "../../common/logger";
import { customResponse } from "../../common/responses";
import { v4 as uuid } from "uuid";
const Kafka = require("node-rdkafka");
console.log(Kafka.features);

/**
 * This returns a producer object that is connected to broker and ready to use for publish
 * @returns Producer object
 */
export const generateProducerClient = () => {
  return new Promise((resolve, reject) => {
    try {
      const finalConfigs = {
        "bootstrap.servers": "BROKER_URL",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "SAL_USERNAME/API_KEY",
        "sasl.password": "SASL_PASSWORD/API_SECRET",
      };
      /**
         * Lot more configs can be given here based on need
         */

      const topicOptions = { "request.required.acks": -1 };
      let _producer;
      _producer = new Kafka.Producer(finalConfigs, topicOptions);

      const producerDisconnect = () => {
        logger.error(`Disconnected event for the PRODUCER from the package`);
        _producer.connect();
      };

      const producerThrottle = log => {
        logger.debug(`Throttle event for from the package`, {
          log,
        });
      };

      const producerError = err => {
        const errMsg = (err && err.message) || undefined;
        logger.error(`Error from PRODUCER is:${errMsg}`);
      };

      const producerLog = log => {
        logger.info(`Producer event log notification for debugging:`, log);
      };

      const producerReady = () => {
        try {
          logger.debug(
            `The producer has connected. setting producer ready to true and resolving it`
          );
          resolve(_producer);
        } catch (error) {
          logger.error(
            `Error in producer ready event:${JSON.stringify(
              error
            )} and reconnecting again`
          );
          reject(`Unable to connect because ${JSON.stringify(error)}`);
        }
      };

      _producer.connect();
      console.log(`Connecting to kafka producer`);

      setInterval(() => {
        _producer.setPollInterval(globalConfigs.producerPollingTime);
      }, 2000);

      _producer
        .on(kafkaEvents.READY, producerReady)
        .on(kafkaEvents.LOG, producerLog)
        .on(kafkaEvents.ERROR, producerError)
        .on(kafkaEvents.THROTTLE, producerThrottle)
        .on(kafkaEvents.DISCONNECTED, producerDisconnect);
    } catch (error) {
      logger.error(`Unable to set the configurations for producer client due to missing configs
            please verify with the developers for cluster`);
      reject(`Unable to set the configurations for producer client due to missing configs
            please verify with the developers for cluster`);
    }
  });
};

/**
 *
 * @param {Object} err
 * @param {Object} ackMessage
 */
const generateAcknowledgment = (err, ackMessage) => {
  if (typeof ackMessage.opaque === "function") {
    ackMessage.opaque.call(undefined, err, ackMessage);
  }
};

/**
 *
 * @param {Object} producer
 * @param {String} topic
 * @param {Number} partition
 * @param {Buffer} message
 * @param {String} key
 */
const sendMessagesToKafka = (producer, topic, partition, message, key) => {
  return new Promise((resolve, reject) => {
    // Start sending messages
    try {
      // Register delivery report listener
      producer.on(kafkaEvents.PUBLISH_ACKNOWLEDGMENT, generateAcknowledgment);

      producer.produce(
        topic,
        partition,
        message,
        key,
        Date.now(),
        (err, delvieryReport) => {
          if (err) {
            return reject(`Error publishing message and error is ${err}`);
          }
          if (delvieryReport) {
            const stringifiedValue = delvieryReport.value.toString();

            const resp = customResponse(201, {
              partition: delvieryReport.partition,
              offset: delvieryReport.offset,
              key: delvieryReport.key.toString(),
              value: JSON.parse(stringifiedValue),
            });

            return resolve(resp);
          }
        }
      );
    } catch (err) {
      logger.error(`Failed publishing message: ${message} to kafka as ${err}`);
      throw err;
    }
  });
};

export class PublishServiceImpl {
  // eslint-disable-line
  postMessageToPublisher = (req, res) => {
    return new Promise((resolve, reject) => {
      try {
        const requestBody = req.body;

        const actualBody = requestBody.data;

        const key = `${uuid()}`;

        const partition = requestBody.partition;

        const data = Buffer.from(JSON.stringify(actualBody));

        generateProducerClient(topicName)
          .then(producerClient => {
            logger.debug(
              "Obtained the producer client so starting the publish process"
            );
            /**
             * Start processing the kafka messages
             */
            sendMessagesToKafka(producerClient, topicName, partition, data, key)
              .then(dataVal => {
                logger.info(
                  `Published Message and partition is ${dataVal.partition} and offset is ${dataVal.offset}
                for topic ${topicName}`
                );
                return resolve(dataVal);
              })
              .catch(err => {
                logger.error(`publishing message error: ${err}`);
                return reject(err);
              });
          })
          .catch(issue => {
            logger.error(`Unable to connect to producer client as: ${issue}`);
            return reject(issue);
          });
      } catch (error) {
        return reject(error);
      }
    });
  };
}
