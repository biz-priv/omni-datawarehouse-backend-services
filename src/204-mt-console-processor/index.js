'use strict';

const { get, pickBy } = require('lodash');
const AWS = require('aws-sdk');
const { STATUSES, TABLE_PARAMS, TYPES } = require('../shared/constants/204_create_shipment');

const { SNS_TOPIC_ARN, STATUS_TABLE } = process.env;

const dynamoDb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

let functionName;
module.exports.handler = async (event, context) => {
  try {
    functionName = get(context, 'functionName');
    console.info('🙂 -> file: index.js:8 -> functionName:', functionName);
    const pendingStatus = await queryTableStatusPending();
    await Promise.all([
      ...pendingStatus.filter(({ Type }) => Type === TYPES.MULTI_STOP).map(checkMultiStop),
    ]);
    return 'success';
  } catch (e) {
    console.error('🚀 ~ file: 204 table status:32 = ~ e:', e);
    await publishSNSTopic({
      message: ` ${e.message}
      \n Please check details on ${'live-204-console-status-table-dev'}. Look for status FAILED.
      \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0`,
    });
    return false;
  }
};

async function publishSNSTopic({ message }) {
  // return;
  await sns
    .publish({
      TopicArn: SNS_TOPIC_ARN,
      Subject: `Error on ${functionName} lambda.`,
      Message: `An error occurred: ${message}`,
    })
    .promise();
}

async function queryTableStatusPending() {
  const params = {
    TableName: 'live-204-console-status-table-dev',
    IndexName: 'Status-index',
    KeyConditionExpression: '#Status = :status',
    ExpressionAttributeNames: { '#Status': 'Status' },
    ExpressionAttributeValues: {
      ':status': STATUSES.PENDING,
    },
  };

  try {
    const data = await dynamoDb.query(params).promise();
    console.info('Query succeeded:', data);
    return get(data, 'Items', []);
  } catch (err) {
    console.info('Query error:', err);
    throw err;
  }
}

async function checkMultiStop(tableData) {
  console.info('🙂 -> file: index.js:80 -> tableData:', tableData);
  const originalTableStatuses = { ...get(tableData, 'TableStatuses', {}) };
  const type = get(tableData, 'Type');
  console.info('🙂 -> file: index.js:83 -> type:', type);
  const orderNo = get(tableData, 'FK_OrderNo');
  console.info('🙂 -> file: index.js:85 -> orderNo:', orderNo);
  const consoleNo = get(tableData, 'ShipmentAparData.ConsolNo');
  console.info('🙂 -> file: index.js:87 -> consoleNo:', consoleNo);
  const retryCount = get(tableData, 'RetryCount', 0);
  console.info('🙂 -> file: index.js:90 -> retryCount:', retryCount);
  const orderNumbersForConsol = Object.keys(get(tableData, 'TableStatuses'));
  const result = await Promise.all(
    orderNumbersForConsol.map(async (orderNoForConsol) => {
      console.info('🙂 -> file: index.js:160 -> orderNoForConsol:', orderNoForConsol);
      const tableNames = Object.keys(
        pickBy(
          get(tableData, `TableStatuses.${orderNoForConsol}`, {}),
          (value) => value === STATUSES.PENDING
        )
      );
      console.info('🙂 -> file: index.js:81 -> tableName:', tableNames);
      await Promise.all(
        tableNames.map(async (tableName) => {
          try {
            console.info('🙂 -> file: index.js:86 -> tableName:', tableName);
            const param = TABLE_PARAMS[type][tableName]({ orderNo: orderNoForConsol, consoleNo });
            originalTableStatuses[`${orderNoForConsol}`][tableName] = await fetchItemFromTable({
              params: param,
            });
          } catch (error) {
            console.info('🙂 -> file: index.js:182 -> error:', error);
            throw error;
          }
        })
      );
    })
  );
  console.info('🙂 -> file: index.js:177 -> result:', result);
  console.info('🙂 -> file: index.js:149 -> originalTableStatuses:', originalTableStatuses);

  for (const key in originalTableStatuses) {
    if (Object.hasOwnProperty.call(originalTableStatuses, key)) {
      const tableStatuses = originalTableStatuses[key];
      if (Object.values(tableStatuses).includes(STATUSES.PENDING) && retryCount < 5) {
        return await updateStatusTable({
          orderNo,
          originalTableStatuses,
          retryCount,
          status: STATUSES.PENDING,
        });
      }

      if (Object.values(tableStatuses).includes(STATUSES.PENDING) && retryCount >= 5) {
        await publishSNSTopic({
          message: `All tables are not populated for order id: ${orderNo}. 
              \n Please check ${STATUS_TABLE} to see which table does not have data. 
              \n Retrigger the process by changing Status to ${STATUSES.PENDING} and resetting the RetryCount to 0.`,
        });
        return await updateStatusTable({
          orderNo,
          originalTableStatuses,
          retryCount,
          status: STATUSES.FAILED,
        });
      }
    }
  }

  return await updateStatusTable({
    orderNo,
    originalTableStatuses,
    retryCount,
    status: STATUSES.READY,
  });
}

async function fetchItemFromTable({ params }) {
  try {
    console.info('🙂 -> file: index.js:135 -> params:', params);
    const data = await dynamoDb.query(params).promise();
    console.info('🙂 -> file: index.js:138 -> fetchItemFromTable -> data:', data);
    return get(data, 'Items', []).length > 0 ? STATUSES.READY : STATUSES.PENDING;
  } catch (err) {
    if (err.code === 'ResourceNotFoundException') {
      return STATUSES.PENDING;
    }
    throw err;
  }
}

async function updateStatusTable({ orderNo, originalTableStatuses, retryCount, status }) {
  const updateParam = {
    TableName: 'live-204-console-status-table-dev',
    Key: { FK_OrderNo: orderNo },
    UpdateExpression:
      'set TableStatuses = :tableStatuses, RetryCount = :retryCount, #Status = :status',
    ExpressionAttributeNames: { '#Status': 'Status' },
    ExpressionAttributeValues: {
      ':tableStatuses': originalTableStatuses,
      ':retryCount': retryCount + 1,
      ':status': status,
    },
  };
  console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
  return await dynamoDb.update(updateParam).promise();
}
