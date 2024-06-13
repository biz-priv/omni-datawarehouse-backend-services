/*
* File: src\204-update-shipment-stream-processor\index.js
* Project: Omni-datawarehouse-backend-services
* Author: Bizcloud Experts
* Date: 2024-05-03
* Confidential and Proprietary
*/
'use strict';

const AWS = require('aws-sdk');
const _ = require('lodash');

const sns = new AWS.SNS();
const dynamoDb = new AWS.DynamoDB.DocumentClient();

// Constants from environment variables
const { STATUS_TABLE, CONSOL_STOP_HEADERS, CONSOLE_STATUS_TABLE, LIVE_SNS_TOPIC_ARN } = process.env;

// Constants from shared files
const { STATUSES } = require('../shared/constants/204_create_shipment');

let functionName;
module.exports.handler = async (event, context) => {
  functionName = _.get(context, 'functionName');
  console.info('🙂 -> file: index.js:8 -> functionName:', functionName);
  try {
    console.info('Event:', event);

    await Promise.all(_.map(event.Records, processRecord));
  } catch (error) {
    console.error('Error processing records:', error);
    throw error;
  }
};

async function processRecord(record) {
  try {
    const body = JSON.parse(_.get(record, 'body', ''));
    const message = JSON.parse(_.get(body, 'Message', ''));

    const newImage = AWS.DynamoDB.Converter.unmarshall(_.get(message, 'NewImage', {}));
    // const oldImage = AWS.DynamoDB.Converter.unmarshall(_.get(message, 'OldImage', {}));
    const dynamoTableName = _.get(message, 'dynamoTableName', '');

    if (dynamoTableName === CONSOL_STOP_HEADERS) {
      await handleUpdatesforMt(newImage);
    } else {
      await handleUpdatesForP2P(newImage);
    }
  } catch (error) {
    console.error('Error processing record:', error);
    throw error;
  }
}

async function publishSNSTopic({ message, stationCode, subject }) {
  try {
    const params = {
      TopicArn: LIVE_SNS_TOPIC_ARN,
      Subject: subject,
      Message: `An error occurred in ${functionName}: \n${message}`,
      MessageAttributes: {
        stationCode: {
          DataType: 'String',
          StringValue: stationCode.toString(),
        },
      },
    };

    await sns.publish(params).promise();
  } catch (error) {
    console.error('Error publishing to SNS topic:', error);
    throw error;
  }
}

async function handleUpdatesForP2P(newImage) {
  const orderNo = _.get(newImage, 'FK_OrderNo');
  const consolNo = _.get(newImage, 'ConsolNo');

  try {
    // Query log table
    const logQueryResult = await queryOrderStatusTable(orderNo);
    const status = _.get(logQueryResult, '[0].Status', '');

    if (!_.isEmpty(logQueryResult) && status === STATUSES.FAILED) {
      await updateOrderStatusTable({ orderNo });
      console.info('retriggered the process successfully');
    }
  } catch (error) {
    console.error('Error handling order updates:', error);
    await publishSNSTopic({
      message: `${error.message}`,
      stationCode: 'SUPPORT',
      subject: `An error occured while retriggering the process ~ Consol: ${consolNo} / FK_OrderNo: ${orderNo}`,
    });
    throw error;
  }
}

async function handleUpdatesforMt(newImage) {
  const consolNo = _.get(newImage, 'FK_ConsolNo');

  try {
    const logQueryResult = await queryConsolStatusTable(consolNo);
    if (!_.isEmpty(logQueryResult) && _.get(logQueryResult, '[0]Status') === STATUSES.FAILED) {
      // retrigger the process by updating the status from 'FAILED' to 'PENDING'
      await updateConsolStatusTable({ consolNo });
      console.info('retriggered the process successfully');
    }

    return true;
  } catch (error) {
    console.error('Error handling consolidation updates:', error);
    await publishSNSTopic({
      message: `${error.message}`,
      stationCode: 'SUPPORT',
      subject: `An error occured while retriggering the process ~ ConsolNo: ${consolNo} `,
    });
    throw error;
  }
}

const queryOrderStatusTable = async (orderNo) => {
  const orderParam = {
    TableName: STATUS_TABLE,
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };
  console.info('orderParam:', orderParam);
  try {
    const result = await dynamoDb.query(orderParam).promise();
    return _.get(result, 'Items', []);
  } catch (error) {
    console.error('Error querying status table:', error);
    throw error;
  }
};

async function updateOrderStatusTable({ orderNo }) {
  const updateParams = {
    TableName: STATUS_TABLE,
    Key: { FK_OrderNo: orderNo },
    UpdateExpression: 'set #Status = :status, ResetCount = :resetCount',
    ExpressionAttributeNames: { '#Status': 'Status' },
    ExpressionAttributeValues: {
      ':status': STATUSES.PENDING,
      ':resetCount': 0,
    },
  };
  console.info('Update status table parameters:', updateParams);
  try {
    await dynamoDb.update(updateParams).promise();
  } catch (error) {
    console.error('Error updating status table:', error);
    throw error;
  }
}

async function queryConsolStatusTable(consolNo) {
  try {
    const queryParams = {
      TableName: CONSOLE_STATUS_TABLE,
      KeyConditionExpression: 'ConsolNo = :consolNo',
      ExpressionAttributeValues: {
        ':consolNo': consolNo,
      },
    };
    console.info('Query status table parameters:', queryParams);
    const result = await dynamoDb.query(queryParams).promise();
    return _.get(result, 'Items', []);
  } catch (error) {
    console.error('🚀 ~ file: index.js:460 ~ queryConsolStatusTable ~ error:', error);
    throw error;
  }
}

async function updateConsolStatusTable({ consolNo }) {
  try {
    const updateParams = {
      TableName: CONSOLE_STATUS_TABLE,
      Key: { ConsolNo: consolNo },
      UpdateExpression: 'set #Status = :status, ResetCount = :resetCount',
      ExpressionAttributeNames: { '#Status': 'Status' },
      ExpressionAttributeValues: {
        ':status': STATUSES.PENDING,
        ':resetCount': 0,
      },
    };
    console.info('Update status table parameters:', updateParams);
    await dynamoDb.update(updateParams).promise();
    return 'success';
  } catch (error) {
    console.error('🚀 ~ file: index.js:481 ~ updateConsolStatusTable ~ error:', error);
    throw error;
  }
}
