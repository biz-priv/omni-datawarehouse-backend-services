'use strict';
const { get, pickBy } = require('lodash');
const AWS = require('aws-sdk');
const { STATUSES, TABLE_PARAMS, TYPES } = require('../shared/constants/204_create_shipment');

const { SNS_TOPIC_ARN, STATUS_TABLE, STATUS_TABLE_STATUS_INDEX } = process.env;
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

let functionName;
module.exports.handler = async (event, context) => {
  try {
    console.info(
      '🙂 -> file: index.js:13 -> STATUS_TABLE_STATUS_INDEX:',
      STATUS_TABLE_STATUS_INDEX
    );
    console.info('🙂 -> file: index.js:13 -> STATUS_TABLE:', STATUS_TABLE);
    functionName = get(context, 'functionName');
    console.info('🙂 -> file: index.js:8 -> functionName:', functionName);
    console.info(
      '🚀 ~ file: shipment_header_table_stream_processor.js:8 ~ exports.handler= ~ event:',
      JSON.stringify(event)
    );
    const statusTableResult = await queryTableStatusIndex();
    await Promise.all(
      statusTableResult.filter(({ Type }) => Type !== TYPES.MULTI_STOP).map(checkTable)
    );
    return false;
  } catch (e) {
    console.error(
      '🚀 ~ file: shipment_header_table_stream_processor.js:117 ~ module.exports.handler= ~ e:',
      e
    );
    return false;
  }
};

async function publishSNSTopic({ message }) {
  // return;
  await sns
    .publish({
      // TopicArn: 'arn:aws:sns:us-east-1:332281781429:omni-error-notification-topic-dev',
      TopicArn: SNS_TOPIC_ARN,
      Subject: `Error on ${functionName} lambda.`,
      Message: `An error occurred: ${message}`,
    })
    .promise();
}

async function queryTableStatusIndex() {
  const params = {
    TableName: STATUS_TABLE,
    IndexName: STATUS_TABLE_STATUS_INDEX,
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

async function checkTable(tableData) {
  console.info('🙂 -> file: index.js:80 -> tableData:', tableData);
  const originalTableStatuses = { ...get(tableData, 'TableStatuses', {}) };
  const tableNames = Object.keys(
    pickBy(get(tableData, 'TableStatuses', {}), (value) => value === STATUSES.PENDING)
  );
  console.info('🙂 -> file: index.js:81 -> tableName:', tableNames);
  const type = get(tableData, 'Type');
  console.info('🙂 -> file: index.js:83 -> type:', type);
  const orderNo = get(tableData, 'FK_OrderNo');
  console.info('🙂 -> file: index.js:85 -> orderNo:', orderNo);
  const consoleNo = get(tableData, 'ShipmentAparData.ConsolNo');
  console.info('🙂 -> file: index.js:87 -> consoleNo:', consoleNo);
  const retryCount = get(tableData, 'RetryCount', 0);
  console.info('🙂 -> file: index.js:90 -> retryCount:', retryCount);

  await Promise.all(
    tableNames.map(async (tableName) => {
      console.info('🙂 -> file: index.js:86 -> tableName:', tableName);
      const param = TABLE_PARAMS[type][tableName]({ orderNo, consoleNo });
      originalTableStatuses[tableName] = await fetchItemFromTable({ params: param });
    })
  );
  console.info('🙂 -> file: index.js:79 -> originalTableStatuses:', originalTableStatuses);
  if (Object.values(originalTableStatuses).includes(STATUSES.PENDING) && retryCount >= 5) {
    const updateParam = {
      TableName: STATUS_TABLE,
      Key: { FK_OrderNo: orderNo },
      UpdateExpression:
        'set TableStatuses = :tableStatuses, RetryCount = :retryCount, #Status = :status',
      ExpressionAttributeNames: { '#Status': 'Status' },
      ExpressionAttributeValues: {
        ':tableStatuses': originalTableStatuses,
        ':retryCount': retryCount + 1,
        ':status': STATUSES.FAILED,
      },
    };
    console.info('🙂 -> file: index.js:113 -> updateParam:', updateParam);
    await dynamoDb.update(updateParam).promise();
    await publishSNSTopic({
      message: `All tables are not populated for order id: ${orderNo}. 
      \n Please check ${STATUS_TABLE} to see which table does not have data. 
      \n Retrigger the process by changes Status to ${STATUSES.PENDING}`,
    });
    return false;
  }
  if (Object.values(originalTableStatuses).includes(STATUSES.PENDING)) {
    const updateParam = {
      TableName: STATUS_TABLE,
      Key: { FK_OrderNo: orderNo },
      UpdateExpression: 'set TableStatuses = :tableStatuses, RetryCount = :retryCount',
      ExpressionAttributeValues: {
        ':tableStatuses': originalTableStatuses,
        ':retryCount': retryCount + 1,
      },
    };
    console.info('🙂 -> file: index.js:113 -> updateParam:', updateParam);
    await dynamoDb.update(updateParam).promise();
    return false;
  }
  if (
    (type === TYPES.CONSOLE || type === TYPES.NON_CONSOLE) &&
    get(originalTableStatuses, 'tbl_ConfirmationCost') === STATUSES.PENDING &&
    get(originalTableStatuses, 'tbl_Shipper') === STATUSES.READY &&
    get(originalTableStatuses, 'tbl_Consignee') === STATUSES.READY &&
    retryCount <= 3
  ) {
    const updateParam = {
      TableName: STATUS_TABLE,
      Key: { FK_OrderNo: orderNo },
      UpdateExpression:
        'set TableStatuses = :tableStatuses, RetryCount = :retryCount, #Status = :status',
      ExpressionAttributeNames: { '#Status': 'Status' },
      ExpressionAttributeValues: {
        ':tableStatuses': originalTableStatuses,
        ':retryCount': retryCount + 1,
        ':status': STATUSES.READY,
      },
    };
    console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
    await dynamoDb.update(updateParam).promise();
    return true;
  }
  const updateParam = {
    TableName: STATUS_TABLE,
    Key: { FK_OrderNo: orderNo },
    UpdateExpression:
      'set TableStatuses = :tableStatuses, RetryCount = :retryCount, #Status = :status',
    ExpressionAttributeNames: { '#Status': 'Status' },
    ExpressionAttributeValues: {
      ':tableStatuses': originalTableStatuses,
      ':retryCount': retryCount + 1,
      ':status': STATUSES.READY,
    },
  };
  console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
  await dynamoDb.update(updateParam).promise();
  return true;
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
