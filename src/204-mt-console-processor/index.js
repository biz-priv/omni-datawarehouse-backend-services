'use strict';

const { get, pickBy, includes } = require('lodash');
const AWS = require('aws-sdk');
const {
  STATUSES,
  TABLE_PARAMS,
  TYPES,
  CONSOLE_WISE_REQUIRED_FIELDS,
} = require('../shared/constants/204_create_shipment');
const moment = require('moment-timezone');

const { LIVE_SNS_TOPIC_ARN, STATUS_TABLE, CONSOLE_STATUS_TABLE, CONSOL_STOP_HEADERS, STAGE } =
  process.env;

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
      TopicArn: LIVE_SNS_TOPIC_ARN,
      Subject: `POWERBROKER ERROR NOTIFIACATION - ${STAGE}`,
      Message: `An error occurred in ${functionName}: ${message}`,
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

  for (const key in originalTableStatuses) {
    if (Object.hasOwn(originalTableStatuses, key)) {
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
        const pendingTables = Object.keys(tableStatuses).filter((table) => {
          return tableStatuses[table] === STATUSES.PENDING;
        });
        let missingFields = pendingTables.map(
          (pendingTable) => CONSOLE_WISE_REQUIRED_FIELDS[type][pendingTable]
        );
        missingFields = missingFields.flat().join('\n');
        await publishSNSTopic({
          message: `All tables are not populated for consolNo: ${consolNo}.
            \n Please check if all the below feilds are populated: 
              \n ${missingFields} 
              \n Please check ${CONSOLE_STATUS_TABLE} to see which table does not have data. 
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
    if (get(params, 'TableName', '') === CONSOL_STOP_HEADERS) {
      const data = await dynamoDb.query(params).promise();
      console.info('🙂 -> file: index.js:138 -> fetchItemFromTable -> data:', data);
      const stopHeaders = get(data, 'Items', []);
      if (
        stopHeaders.every(
          (item) =>
            item.FK_ConsolStopState &&
            item.ConsolStopTimeEnd &&
            item.ConsolStopTimeBegin &&
            item.ConsolStopName &&
            item.ConsolStopDate &&
            item.ConsolStopCity &&
            item.ConsolStopAddress1
        )
      ) {
        return STATUSES.READY;
      }
      return STATUSES.PENDING;
    }
    // For other tables, proceed with regular logic
    const data = await dynamoDb.query(params).promise();
    console.info('🙂 -> file: index.js:138 -> fetchItemFromTable -> data:', data);
    return get(data, 'Items', []).length > 0 ? STATUSES.READY : STATUSES.PENDING;
  } catch (err) {
    console.error('error in fetchItemFromTable function - index.js 153:', err);
    if (err.code === 'ResourceNotFoundException') {
      return STATUSES.PENDING;
    }
    throw err;
  }
}

async function updateConosleStatusTable({ consolNo, originalTableStatuses, retryCount, status }) {
  try {
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
  } catch (error) {
    console.error('error in updateConosleStatusTable - index.js ~ 180: ', error);
    throw error;
  }
}

async function updateOrderStatusTable({ orderNo, originalTableStatuses }) {
  try {
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
  } catch (error) {
    console.error('🚀 ~ file: index.js:207 ~ error in updateOrderStatusTable:', error);
    throw error;
  }
}

async function checkAndUpdateOrderTable({ orderNo, originalTableStatuses }) {
  try {
    const existingOrderParam = {
      TableName: STATUS_TABLE,
      KeyConditionExpression: 'FK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    };
    console.info(
      '🚀 ~ file: index.js:221 ~ checkAndUpdateOrderTable ~ existingOrderParam:',
      existingOrderParam
    );

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
  } catch (error) {
    console.error('🚀 ~ file: index.js:240 ~ checkAndUpdateOrderTable ~ error:', error);
    throw error;
  }
}

async function sleep(seconds) {
  try {
    return new Promise((resolve) => setTimeout(resolve, seconds * 1000));
  } catch (error) {
    console.error('🚀 ~ file: index.js:249 ~ sleep ~ error:', error);
    throw error;
  }
}
