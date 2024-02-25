'use strict';

const { get, pickBy } = require('lodash');
const AWS = require('aws-sdk');
const {
  STATUSES,
  TABLE_PARAMS,
  TYPES,
  CONSOLE_WISE_REQUIRED_FIELDS,
} = require('../shared/constants/204_create_shipment');

const dynamoDb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

const { STATUS_TABLE, SNS_TOPIC_ARN, STAGE } = process.env;

let functionName;
module.exports.handler = async (event, context) => {
  try {
    functionName = get(context, 'functionName');
    console.info('🙂 -> file: index.js:8 -> functionName:', functionName);
    const pendingStatusResult = await queryTableStatusPending();
    await Promise.all([
      ...pendingStatusResult.filter(({ Type }) => Type !== TYPES.MULTI_STOP).map(checkTable),
    ]);
    return 'success';
  } catch (e) {
    console.error('🚀 ~ file: 204 table status:32 = ~ e:', e);
    await publishSNSTopic({
      message: ` ${e.message}
      \n Please check details on ${STATUS_TABLE}. Look for status FAILED.
      \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0`,
    });
    return false;
  }
};

async function publishSNSTopic({ message }) {
  try {
    await sns
      .publish({
        TopicArn: SNS_TOPIC_ARN,
        Subject: `POWERBROKER ERROR NOTIFIACATION - ${STAGE}`,
        Message: `An error occurred in ${functionName}: 
        \n${message}`,
      })
      .promise();
  } catch (error) {
    console.error('🚀 ~ file: index.js:43 ~ publishSNSTopic ~ error:', error);
    throw error;
  }
}

async function queryTableStatusPending() {
  const params = {
    TableName: STATUS_TABLE,
    IndexName: 'Status-index',
    KeyConditionExpression: '#Status = :status',
    ExpressionAttributeNames: { '#Status': 'Status' },
    ExpressionAttributeValues: {
      ':status': STATUSES.PENDING,
    },
  };
  console.info('🚀 ~ file: index.js:58 ~ queryTableStatusPending ~ params:', params);
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
      try {
        console.info('🙂 -> file: index.js:86 -> tableName:', tableName);
        const param = TABLE_PARAMS[type][tableName]({ orderNo, consoleNo });
        originalTableStatuses[tableName] = await fetchItemFromTable({ params: param });
      } catch (error) {
        console.info('🙂 -> file: index.js:95 -> error:', error);
        throw error;
      }
    })
  );
  console.info('🙂 -> file: index.js:79 -> originalTableStatuses:', originalTableStatuses);
  if (Object.values(originalTableStatuses).includes(STATUSES.PENDING) && retryCount >= 5) {
    const pendingTables = Object.keys(originalTableStatuses).filter((table) => {
      return originalTableStatuses[table] === STATUSES.PENDING;
    });
    await updateStatusTable({
      orderNo,
      originalTableStatuses,
      retryCount,
      status: STATUSES.FAILED,
    });
    let missingFields = pendingTables.map(
      (pendingTable) => CONSOLE_WISE_REQUIRED_FIELDS[type][pendingTable]
    );
    missingFields = missingFields.flat().join('\n');
    await publishSNSTopic({
      message: `All tables are not populated for order id: ${orderNo}.
      \n Please check if all the below feilds are populated: 
      \n${missingFields}. 
      \n Please check ${STATUS_TABLE} to see which table does not have data. 
      \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0`,
    });
    return false;
  }

  if (
    type === TYPES.NON_CONSOLE &&
    get(originalTableStatuses, 'tbl_ConfirmationCost') === STATUSES.PENDING &&
    get(originalTableStatuses, 'tbl_Shipper') === STATUSES.READY &&
    get(originalTableStatuses, 'tbl_Consignee') === STATUSES.READY &&
    retryCount <= 3 &&
    Object.keys(
      pickBy(get(originalTableStatuses, 'TableStatuses', {}), (value) => value === STATUSES.PENDING)
    ).length === 1
  ) {
    return await updateStatusTable({
      orderNo,
      originalTableStatuses,
      retryCount,
      status: STATUSES.READY,
    });
  }

  if (Object.values(originalTableStatuses).includes(STATUSES.PENDING) && retryCount < 5) {
    return await updateStatusTable({
      orderNo,
      originalTableStatuses,
      retryCount,
      status: STATUSES.PENDING,
    });
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
    console.error('🚀 ~ file: index.js:159 ~ fetchItemFromTable ~ err:', err);
    if (err.code === 'ResourceNotFoundException') {
      return STATUSES.PENDING;
    }
    throw err;
  }
}

async function updateStatusTable({ orderNo, originalTableStatuses, retryCount, status }) {
  try {
    const updateParam = {
      TableName: STATUS_TABLE,
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
  } catch (error) {
    console.error('🚀 ~ file: index.js:184 ~ updateStatusTable ~ error:', error);
    throw error;
  }
}
