'use strict';

const { get } = require('lodash');
const AWS = require('aws-sdk');

const dynamoDb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

const { LOGS_TABLE } = process.env;

let functionName;
module.exports.handler = async (event, context) => {
  try {
    functionName = get(context, 'functionName');
    console.info('functionName:', functionName);
    const failedStatusResult = await queryTableStatusPending();
    if (!failedStatusResult || failedStatusResult.length === 0) {
      console.info('No records failed, Skipping');
      return 'No records failed, Skipping';
    }
    console.info('failedStatusResult:', failedStatusResult);
    const updatePromises = failedStatusResult.map((record) => updateRecord(record));
    await Promise.all(updatePromises);
    return 'success';
  } catch (e) {
    console.error('Error while updating status as READY for FAILED records', e);

    try {
      const params = {
        Message: `An error occurred in function ${context.functionName}.\n\nERROR DETAILS: ${e}.\n\n`,
        Subject: `CW to WT Create Shipment retry process ERROR ${context.functionName}`,
        TopicArn: process.env.NOTIFICATION_ARN,
      };
      await sns.publish(params).promise();
      console.info('SNS notification has sent');
    } catch (err) {
      console.error('Error while sending sns notification: ', err);
    }
    return 'failed';
  }
};

async function queryTableStatusPending() {
  const params = {
    TableName: LOGS_TABLE,
    IndexName: 'Status-RetryCount-Index',
    KeyConditionExpression: '#status = :status AND #retrycount = :retrycount',
    ExpressionAttributeNames: {
      '#status': 'Status',
      '#retrycount': 'RetryCount',
    },
    ExpressionAttributeValues: {
      ':status': 'FAILED',
      ':retrycount': '0',
    },
  };
  console.info('params:', params);
  try {
    const data = await dynamoDb.query(params).promise();
    console.info('Query succeeded:', data);
    return get(data, 'Items', []);
  } catch (err) {
    console.info('Query error:', err);
    throw err;
  }
}

async function updateRecord(record) {
  try {
    const params = {
      TableName: LOGS_TABLE,
      Key: { Id: record.Id },
      UpdateExpression: 'SET #statusAttr = :newStatus',
      ExpressionAttributeNames: {
        '#statusAttr': 'Status',
      },
      ExpressionAttributeValues: {
        ':newStatus': 'READY',
      },
    };

    await dynamoDb.update(params).promise();
    console.info('Record updated successfully:', record);
  } catch (err) {
    console.error('Error updating record:', err);
    throw err;
  }
}
