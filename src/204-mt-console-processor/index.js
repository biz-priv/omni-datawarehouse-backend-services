'use strict';

const { get, pickBy, includes } = require('lodash');
const AWS = require('aws-sdk');
const { STATUSES, TABLE_PARAMS, TYPES } = require('../shared/constants/204_create_shipment');
const moment = require('moment-timezone');

const { SNS_TOPIC_ARN, STATUS_TABLE, CONSOLE_STATUS_TABLE } = process.env;

const dynamoDb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

let functionName;
module.exports.handler = async (event, context) => {
  console.info('🙂 -> file: index.js:14 -> module.exports.handler= -> event:', event);
  try {
    functionName = get(context, 'functionName');
    console.info('🙂 -> file: index.js:8 -> functionName:', functionName);
    await sleep(20); // delay for 30 seconds to let other order for that console to process
    const pendingStatus = await queryTableStatusPending();
    await Promise.all([...pendingStatus.map(checkMultiStop)]);
    return 'success';
  } catch (e) {
    console.error('🚀 ~ file: 204 table status:32 = ~ e:', e);
    await publishSNSTopic({
      message: ` ${e.message}
      \n Please check details on ${CONSOLE_STATUS_TABLE}. Look for status FAILED.
      \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0`,
    });
    return false;
  }
};

async function publishSNSTopic({ message }) {
  return await sns
    .publish({
      TopicArn: SNS_TOPIC_ARN,
      Subject: `Error on ${functionName} lambda.`,
      Message: `An error occurred: ${message}`,
    })
    .promise();
}

async function queryTableStatusPending() {
  const params = {
    TableName: CONSOLE_STATUS_TABLE,
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
  const type = TYPES.MULTI_STOP;
  console.info('🙂 -> file: index.js:83 -> type:', type);
  const consolNo = get(tableData, 'ConsolNo');
  console.info('🙂 -> file: index.js:87 -> consolNo:', consolNo);
  const retryCount = get(tableData, 'RetryCount', 0);
  console.info('🙂 -> file: index.js:90 -> retryCount:', retryCount);
  const orderNumbersForConsol = Object.keys(get(tableData, 'TableStatuses'));
  await Promise.all(
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
            const param = TABLE_PARAMS[type][tableName]({
              orderNo: orderNoForConsol,
              consoleNo: consolNo,
            });
            originalTableStatuses[`${orderNoForConsol}`][tableName] = await fetchItemFromTable({
              params: param,
            });
          } catch (error) {
            console.info('🙂 -> file: index.js:182 -> error:', error);
            throw error;
          }
        })
      );
      await checkAndUpdateOrderTable({
        orderNo: orderNoForConsol,
        originalTableStatuses: originalTableStatuses[orderNoForConsol],
      });
    })
  );
  console.info('🙂 -> file: index.js:149 -> originalTableStatuses:', originalTableStatuses);

  // const statuses = Object.entries(originalTableStatuses).map(([key]) => {
  //   const tableStatuses = originalTableStatuses[key];
  //   if (Object.values(tableStatuses).includes(STATUSES.PENDING) && retryCount < 5) {
  //     return STATUSES.PENDING;
  //   }
  //   if (Object.values(tableStatuses).includes(STATUSES.PENDING) && retryCount >= 5) {
  //     return STATUSES.FAILED;
  //   }
  //   return STATUSES.READY;
  // });

  // console.info('🙂 -> file: index.js:120 -> statuses -> statuses:', statuses);

  // if (statuses.includes(STATUSES.FAILED)) {
  //   await publishSNSTopic({
  //     message: `All tables are not populated for consol No: ${consolNo}.
  //         \n Please check ${STATUS_TABLE} to see which table does not have data.
  //         \n Retrigger the process by changing Status to ${STATUSES.PENDING} and resetting the RetryCount to 0.`,
  //   });
  //   await updateConosleStatusTable({
  //     consolNo,
  //     originalTableStatuses,
  //     retryCount,
  //     status: STATUSES.FAILED,
  //   });
  // }

  // if (
  //   statuses.includes(statuses.includes(STATUSES.PENDING)) &&
  //   !statuses.includes[STATUSES.FAILED]
  // ) {
  //   await updateConosleStatusTable({
  //     consolNo,
  //     originalTableStatuses,
  //     retryCount,
  //     status: STATUSES.PENDING,
  //   });
  // }

  // if (
  //   !statuses.includes(STATUSES.PENDING) &&
  //   !statuses.includes[STATUSES.FAILED] &&
  //   statuses.every((data) => data === STATUSES.READY)
  // ) {
  //   await updateConosleStatusTable({
  //     consolNo,
  //     originalTableStatuses,
  //     retryCount,
  //     status: STATUSES.READY,
  //   });
  // }

  for (const key in originalTableStatuses) {
    if (Object.hasOwnProperty.call(originalTableStatuses, key)) {
      const tableStatuses = originalTableStatuses[key];
      if (Object.values(tableStatuses).includes(STATUSES.PENDING) && retryCount < 5) {
        return await updateConosleStatusTable({
          consolNo,
          originalTableStatuses,
          retryCount,
          status: STATUSES.PENDING,
        });
      }

      if (Object.values(tableStatuses).includes(STATUSES.PENDING) && retryCount >= 5) {
        await publishSNSTopic({
          message: `All tables are not populated for order id: ${consolNo}. 
              \n Please check ${STATUS_TABLE} to see which table does not have data. 
              \n Retrigger the process by changing Status to ${STATUSES.PENDING} and resetting the RetryCount to 0.`,
        });
        return await updateConosleStatusTable({
          consolNo,
          originalTableStatuses,
          retryCount,
          status: STATUSES.FAILED,
        });
      }
    }
  }

  return await updateConosleStatusTable({
    consolNo,
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

async function updateConosleStatusTable({ consolNo, originalTableStatuses, retryCount, status }) {
  const updateParam = {
    TableName: CONSOLE_STATUS_TABLE,
    Key: { ConsolNo: String(consolNo) },
    UpdateExpression:
      'set TableStatuses = :tableStatuses, RetryCount = :retryCount, #Status = :status, LastUpdateBy = :lastUpdateBy, LastUpdatedAt = :lastUpdatedAt',
    ExpressionAttributeNames: { '#Status': 'Status' },
    ExpressionAttributeValues: {
      ':tableStatuses': originalTableStatuses,
      ':retryCount': retryCount + 1,
      ':status': status,
      ':lastUpdateBy': functionName,
      ':lastUpdatedAt': moment.tz('America/Chicago').format(),
    },
  };
  console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
  return await dynamoDb.update(updateParam).promise();
}

async function updateOrderStatusTable({ orderNo, originalTableStatuses }) {
  const status = !includes(originalTableStatuses, STATUSES.PENDING)
    ? STATUSES.READY
    : STATUSES.PENDING;

  const updateParam = {
    TableName: STATUS_TABLE,
    Key: { FK_OrderNo: orderNo },
    UpdateExpression:
      'set TableStatuses = :tableStatuses, #Status = :status ,LastUpdateBy = :lastUpdateBy, LastUpdatedAt = :lastUpdatedAt',
    ExpressionAttributeNames: { '#Status': 'Status' },
    ExpressionAttributeValues: {
      ':tableStatuses': originalTableStatuses,
      ':status': status,
      ':lastUpdateBy': functionName,
      ':lastUpdatedAt': moment.tz('America/Chicago').format(),
    },
  };
  console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
  return await dynamoDb.update(updateParam).promise();
}

async function checkAndUpdateOrderTable({ orderNo, originalTableStatuses }) {
  const existingOrderParam = {
    TableName: STATUS_TABLE,
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };

  const existingOrder = await fetchItemFromTable({ params: existingOrderParam });
  console.info(
    '🙂 -> file: index.js:65 -> module.exports.handler= -> existingOrder:',
    existingOrder
  );

  if (existingOrder === STATUSES.PENDING) {
    return false;
  }

  if (existingOrder === STATUSES.READY) {
    return await updateOrderStatusTable({
      orderNo,
      originalTableStatuses,
    });
  }
  return true;
}

async function sleep(seconds) {
  return new Promise((resolve) => setTimeout(resolve, seconds * 1000));
}
