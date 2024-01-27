'use strict';
const { get } = require('lodash');
const AWS = require('aws-sdk');
// const {
//   STATUSES,
//   TYPES,
//   CONSOLE_WISE_TABLES,
//   VENDOR,
// } = require('../shared/constants/204_create_shipment');
// const moment = require('moment-timezone');

const { SNS_TOPIC_ARN, STAGE } = process.env;
// const dynamoDb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

let functionName;
let orderId;
module.exports.handler = async (event, context) => {
  try {
    functionName = get(context, 'functionName');
    console.info('🙂 -> file: index.js:8 -> functionName:', functionName);
    console.info(
      '🚀 ~ file: shipment_header_table_stream_processor.js:8 ~ exports.handler= ~ event:',
      JSON.stringify(event)
    );
    return false;
  } catch (e) {
    console.error(
      '🚀 ~ file: shipment_header_table_stream_processor.js:117 ~ module.exports.handler= ~ e:',
      e
    );
    return await publishSNSTopic({
      message: `Error processing order id: ${orderId}, ${e.message}. \n Please retrigger the process by changing any field in omni-wt-rt-shipment-apar-${STAGE} after fixing the error.`,
    });
  }
};

async function publishSNSTopic({ message }) {
  await sns
    .publish({
      // TopicArn: 'arn:aws:sns:us-east-1:332281781429:omni-error-notification-topic-dev',
      TopicArn: SNS_TOPIC_ARN,
      Subject: `Error on ${functionName} lambda.`,
      Message: `An error occurred: ${message}`,
    })
    .promise();
}
