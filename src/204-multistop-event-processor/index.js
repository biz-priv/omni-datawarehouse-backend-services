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
  console.info('🚀 ~ file: index.js:9 ~ event:', JSON.stringify(event));
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

    // Construct params for the query
    const queryParams = {
      TableName: STATUS_TABLE,
      KeyConditionExpression: 'FK_OrderNo = :consolNo',
      ExpressionAttributeValues: {
        ':consolNo': consolNo,
      },
    };

    // Execute the query
    const queryResult = await dynamoDb.query(queryParams).promise();
    console.info('🚀 ~ file: index.js:92 ~ queryResult:', queryResult);

    let params;
    if (queryResult.Items && queryResult.Items.length > 0) {
      // Record with the FK_OrderNo exists, update the fields
      params = {
        TableName: STATUS_TABLE,
        Key: {
          FK_OrderNo: consolNo,
        },
        UpdateExpression: `
      set #Status = :status,
      #Type = :type,
      #TableStatuses = :tableStatuses,
      #CreatedAt = :createdAt,
      #LastUpdateBy = :lastUpdateBy,
      #LastUpdatedAt = :lastUpdatedAt,
      #ShipmentAparData = :ShipmentAparData
    `,
        ExpressionAttributeNames: {
          '#Status': 'Status',
          '#Type': 'Type',
          '#TableStatuses': 'TableStatuses',
          '#CreatedAt': 'CreatedAt',
          '#LastUpdateBy': 'LastUpdateBy',
          '#LastUpdatedAt': 'LastUpdatedAt',
          '#ShipmentAparData': 'ShipmentAparData',
        },
        ExpressionAttributeValues: {
          ':status': status,
          ':type': type,
          ':tableStatuses': tableStatuses,
          ':createdAt': moment.tz('America/Chicago').format(),
          ':lastUpdateBy': functionName,
          ':lastUpdatedAt': moment.tz('America/Chicago').format(),
          ':ShipmentAparData': ShipmentAparData,
        },
        ReturnValues: 'UPDATED_NEW',
      };

      const result = await dynamoDb.update(params).promise();
      console.info('🚀 ~ file: index.js:133 ~ updated result:', result);
    } else {
      // Record with the FK_OrderNo does not exist, create a new record
      params = {
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

      await dynamoDb.put(params).promise();
    }
  } catch (error) {
    console.error('Error inserting or updating record:', error);
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
