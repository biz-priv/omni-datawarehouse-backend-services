'use strict';
const AWS = require('aws-sdk');
const { get, isEqual } = require('lodash');
// const { updateOrders } = require('../shared/204-payload-generator/apis');

const dynamoDb = new AWS.DynamoDB.DocumentClient();

const { STATUS_TABLE } = process.env;

module.exports.handler = async (event) => {
  await Promise.all(
    event.Records.map(async (record) => {
      const body = JSON.parse(get(record, 'body', ''));
      const message = JSON.parse(get(body, 'Message', ''));
      console.info('🙂 -> file: index.js:9 -> module.exports.handler= -> message:', typeof message);
      const newImage = get(message, 'NewImage', {});
      console.info('🙂 -> file: index.js:11 -> module.exports.handler= -> newImage:', newImage);
      const oldImage = get(message, 'OldImage', {});
      console.info('🙂 -> file: index.js:13 -> module.exports.handler= -> oldImage:', oldImage);
      const dynamoTableName = get(message, 'dynamoTableName', '');
      console.info(
        '🚀 ~ file: index.js:16 ~ event.Records.map ~ dynamoTableName:',
        dynamoTableName
      );
      const orderNo = get(newImage, 'FK_OrderNo');

      // Find changed fields using lodash
      const changedFields = {};
      for (const key in newImage) {
        if (!isEqual(newImage[key], oldImage[key])) {
          changedFields[key] = newImage[key];
        }
      }

      console.info('Changed fields:', changedFields);

      // Query log table
      const logQueryResult = await queryOrderStatusTable(orderNo);
      if (logQueryResult.length === 0) {
        const Response = get(logQueryResult, 'Response');
        console.info('🚀 ~ file: index.js:42 ~ event.Records.map ~ Response:', Response);
        // Send update
        // await updateOrders(updatedResponse);
      } else {
        console.info('Update not found in log table.');
      }
    })
  );
};

const queryOrderStatusTable = async (orderNo) => {
  const orderParam = {
    TableName: STATUS_TABLE,
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };

  try {
    const result = await dynamoDb.query(orderParam).promise();
    return get(result, 'Items', []);
  } catch (error) {
    console.error('Error querying status table:', error);
    throw error;
  }
};
