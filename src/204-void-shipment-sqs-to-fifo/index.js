'use strict';

const AWS = require('aws-sdk');

const sqs = new AWS.SQS();
const _ = require('lodash');

const sns = new AWS.SNS();
const fifoQueueUrl = process.env.QUEUE_URL;

module.exports.handler = async (event, context) => {
  console.info('🚀 ~ file: index.js:10 ~ module.exports.handler= ~ event:', JSON.stringify(event));
  const promises = _.get(event, 'Records', []).map(async (record) => {
    const body = _.get(record, 'body', '');
    console.info(`Received message: ${body}`);

    const params = {
      QueueUrl: fifoQueueUrl,
      MessageBody: body,
      MessageGroupId: 'default',
      MessageDeduplicationId: _.get(record, 'messageId'),
    };

    try {
      const result = await sqs.sendMessage(params).promise();
      console.info(`Message sent to FIFO queue: ${_.get(result, 'MessageId')}`);
    } catch (error) {
      console.error(`Error sending message to FIFO queue: ${_.get(error, 'message')}`);
      await publishSNSTopic({
        message: `An error occured while sending message to FIFO queue.\nError Details:\n${_.get(error, 'message')}`,
        subject: `An Error occured in lambda function ${context.functionName}.`,
      });
    }
  });

  await Promise.all(promises);

  return 'success';
};

async function publishSNSTopic({ message, subject }) {
  try {
    const params = {
      TopicArn: process.env.LIVE_SNS_TOPIC_ARN,
      Subject: subject,
      Message: message,
      MessageAttributes: {
        stationCode: {
          DataType: 'String',
          StringValue: 'SUPPORT',
        },
      },
    };

    await sns.publish(params).promise();
  } catch (error) {
    console.error('Error publishing to SNS topic:', error);
    throw error;
  }
}
