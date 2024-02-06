'use strict';

const AWS = require('aws-sdk');
const { STATUSES, TYPES, CONSOLE_WISE_TABLES } = require('../shared/constants/204_create_shipment');
const _ = require('lodash');
const moment = require('moment-timezone');

const { STATUS_TABLE, SNS_TOPIC_ARN, STAGE } = process.env;
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

let functionName;
let orderIdList;
module.exports.handler = async (event, context) => {
  console.info('🚀 ~ file: index.js:9 ~ event:', event);
  functionName = _.get(context, 'functionName');

  try {
    const promises = _.get(event, 'Records', []).map(async (record) => {
      const newImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
      const oldImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage);
      const newImageStatus = _.get(newImage, 'Status', '');
      const oldImageStatus = _.get(oldImage, 'Status', '');
      const eventName = _.get(record, 'eventName', '');
      if (
        (oldImageStatus === STATUSES.SENT &&
          newImageStatus === STATUSES.PENDING &&
          eventName === 'MODIFY') ||
        eventName === 'INSERT'
      ) {
        // Extract comma-separated FK_OrderNo values and convert to an array
        orderIdList = _.get(newImage, 'FK_OrderNo', '')
          .split(',')
          .map((orderId) => orderId.trim());
        console.info('🙂 -> file: index.js:23 -> orderIdList:', orderIdList);

        const consolNo = parseInt(_.get(newImage, 'ConsolNo', null), 10);
        console.info('🙂 -> file: index.js:23 -> consolNo:', consolNo);
        const aparData = _.get(newImage, 'ShipmentAparData', {});
        // Create a dictionary to store status for each FK_OrderNo
        const tableStatuses = {};

        // Process each orderId using .map
        orderIdList.map(
          async (orderId) => (tableStatuses[orderId] = CONSOLE_WISE_TABLES[TYPES.MULTI_STOP])
        );

        await insertShipmentStatus({
          orderIdList,
          consolNo: consolNo.toString(),
          status: STATUSES.PENDING,
          type: TYPES.MULTI_STOP,
          tableStatuses,
          ShipmentAparData: aparData,
        });
      }
    });

    await Promise.all(promises);
    return `Successfully processed ${_.get(event, 'Records', []).length} records.`;
  } catch (error) {
    console.error('Error processing records:', error);
    return await publishSNSTopic({
      message: `Error processing order id: ${orderIdList}, ${error.message}. \n Please retrigger the process by changing any field in omni-wt-rt-shipment-apar-${STAGE} after fixing the error.`,
    });
  }
};

async function insertShipmentStatus({
  // eslint-disable-next-line no-shadow
  orderIdList,
  consolNo,
  status,
  type,
  tableStatuses,
  ShipmentAparData,
}) {
  try {
    console.info('first orderNo:', orderIdList[0]);
    const params = {
      TableName: STATUS_TABLE,
      Item: {
        FK_OrderNo: consolNo,
        ConsolNo: consolNo,
        Status: status,
        Type: type,
        TableStatuses: tableStatuses,
        ShipmentAparData,
        CreatedAt: moment.tz('America/Chicago').format(),
        LastUpdateBy: functionName,
        LastUpdatedAt: moment.tz('America/Chicago').format(),
      },
    };
    console.info('🙂 -> file: index.js:106 -> params:', params);
    await dynamoDb.put(params).promise();
    console.info('Record created successfully.');
  } catch (error) {
    console.info('🙂 -> file: index.js:78 -> error:', error);
    throw error;
  }
}

async function publishSNSTopic({ message }) {
  await sns
    .publish({
      TopicArn: SNS_TOPIC_ARN,
      Subject: `Error on ${functionName} lambda.`,
      Message: `An error occurred: ${message}`,
    })
    .promise();
}
