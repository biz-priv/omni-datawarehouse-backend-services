'use strict';

const { get, pickBy, every, isEmpty } = require('lodash');
const AWS = require('aws-sdk');
const {
  STATUSES,
  TABLE_PARAMS,
  TYPES,
  CONSOLE_WISE_REQUIRED_FIELDS,
} = require('../shared/constants/204_create_shipment');

const dynamoDb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();
const { getUserEmail, sendSESEmail } = require('../shared/204-payload-generator/helper');

const { STATUS_TABLE, LIVE_SNS_TOPIC_ARN, STAGE, SHIPMENT_HEADER_TABLE, CONFIRMATION_COST } =
  process.env;

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

async function publishSNSTopic({ message, stationCode, subject }) {
  try {
    const params = {
      TopicArn: LIVE_SNS_TOPIC_ARN,
      Subject: subject,
      Message: `An error occurred in ${functionName}: ${message}`,
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
  const type = get(tableData, 'Type');
  const headerResult = await queryDynamoDB(get(tableData, 'FK_OrderNo'));
  console.info('🙂 -> file: index.js:66 -> headerResult:', headerResult);
  const userEmail = await getUserEmail({ userId: get(tableData, 'ShipmentAparData.UpdatedBy') });
  const houseBillString = get(headerResult, '[0]Housebill', 0);
  let handlingStation;
  if (type === TYPES.NON_CONSOLE) {
    handlingStation =
      get(headerResult, '[0]ControllingStation') ??
      get(tableData, 'ShipmentAparData.FK_HandlingStation');
  } else {
    handlingStation = get(tableData, 'ShipmentAparData.FK_ConsolStationId');
  }

  console.info('🚀 ~ file: index.js:84 ~ checkTable ~ handlingStation:', handlingStation);
  try {
    if (handlingStation === '') {
      return '  please Handling Station populate HandlingStation';
    }
    console.info('🙂 -> file: index.js:80 -> tableData:', tableData);
    const originalTableStatuses = { ...get(tableData, 'TableStatuses', {}) };
    const tableNames = Object.keys(
      pickBy(get(tableData, 'TableStatuses', {}), (value) => value === STATUSES.PENDING)
    );
    console.info('🙂 -> file: index.js:81 -> tableName:', tableNames);
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
          originalTableStatuses[tableName] = await fetchItemFromTable({
            params: param,
          });
        } catch (error) {
          console.info('🙂 -> file: index.js:95 -> error:', error);
          throw error;
        }
      })
    );
    console.info('🙂 -> file: index.js:79 -> originalTableStatuses:', originalTableStatuses);

    if (
      type === TYPES.NON_CONSOLE &&
      get(originalTableStatuses, 'tbl_ConfirmationCost') === STATUSES.PENDING &&
      get(originalTableStatuses, 'tbl_Shipper') === STATUSES.READY &&
      get(originalTableStatuses, 'tbl_Consignee') === STATUSES.READY &&
      retryCount >= 3 &&
      Object.values(originalTableStatuses).filter((item) => item === STATUSES.PENDING).length === 1
    ) {
      return await updateStatusTable({
        orderNo,
        originalTableStatuses,
        retryCount,
        status: STATUSES.READY,
      });
    }

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
      if (type === TYPES.CONSOLE) {
        await publishSNSTopic({
          message: `All tables are not populated for ConsolNo: ${orderNo}.
        \n Please check if all the below feilds are populated: 
        \n${missingFields}. 
        \n Please check ${STATUS_TABLE} to see which table does not have data. 
        \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0`,
          stationCode: handlingStation,
          orderNo: get(tableData, 'ShipmentAparData.FK_OrderNo'),
          houseBillString,
          subject: `PB ERROR NOTIFICATION - ${STAGE} ~ ConsolNo: ${orderNo}`,
        });
        await sendSESEmail({
          functionName,
          message: `All tables are not populated for ConsolNo: ${orderNo}.
        \n Please check if all the below feilds are populated: 
        \n${missingFields}. 
        \n Please check ${STATUS_TABLE} to see which table does not have data. 
        \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0`,
          userEmail,
          subject: {
            Data: `PB ERROR NOTIFICATION - ${STAGE} ~ ConsolNo: ${get(tableData, 'ShipmentAparData.FK_OrderNo')}`,
            Charset: 'UTF-8',
          },
        });
      } else {
        await publishSNSTopic({
          message: `All tables are not populated for order id: ${orderNo}.
      \n Please check if all the below feilds are populated: 
      \n${missingFields}. 
      \n Please check ${STATUS_TABLE} to see which table does not have data. 
      \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0`,
          stationCode: handlingStation,
          orderNo: get(tableData, 'ShipmentAparData.FK_OrderNo'),
          houseBillString,
          subject: `PB ERROR NOTIFICATION - ${STAGE} ~  Housebill: ${houseBillString} / FK_OrderNo: ${get(tableData, 'ShipmentAparData.FK_OrderNo')}`,
        });
        await sendSESEmail({
          functionName,
          message: `All tables are not populated for order id: ${orderNo}.
        \n Please check if all the below feilds are populated: 
        \n${missingFields}. 
        \n Please check ${STATUS_TABLE} to see which table does not have data. 
        \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0`,
          userEmail,
          subject: {
            Data: `PB ERROR NOTIFICATION - ${STAGE} ~ Housebill: ${houseBillString} / FK_OrderNo: ${get(tableData, 'ShipmentAparData.FK_OrderNo')}`,
            Charset: 'UTF-8',
          },
        });
      }

      return false;
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
  } catch (error) {
    console.error('🚀 ~ file: index.js:171 ~ checkTable ~ error:', error);
    await publishSNSTopic({
      message: ` ${error.message}
      \n order id: ${get(tableData, 'ShipmentAparData.FK_OrderNo')}
      \n Please check details on ${STATUS_TABLE}. Look for status FAILED.
      \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0`,
      stationCode: handlingStation,
      orderNo: get(tableData, 'ShipmentAparData.FK_OrderNo'),
      houseBillString,
    });
    await sendSESEmail({
      functionName,
      message: ` ${error.message}
      \n order id: ${get(tableData, 'ShipmentAparData.FK_OrderNo')}
      \n Please check details on ${STATUS_TABLE}. Look for status FAILED.
      \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0`,
      userEmail,
      subject: {
        Data: `PB ERROR NOTIFICATION - ${STAGE} ~ Housebill: ${houseBillString} / FK_OrderNo: ${get(tableData, 'ShipmentAparData.FK_OrderNo')}`,
        Charset: 'UTF-8',
      },
    });
    return false;
  }
}

async function fetchItemFromTable({ params }) {
  try {
    console.info('🙂 -> file: index.js:135 -> params:', params);
    if (get(params, 'TableName', '') === CONFIRMATION_COST) {
      const data = await dynamoDb.query(params).promise();
      console.info('🙂 -> file: index.js:138 -> fetchItemFromTable -> data:', data);
      const confirmationCost = get(data, 'Items', []);
      if (
        confirmationCost.length > 0 &&
        confirmationCost.every((item) => {
          const {
            // eslint-disable-next-line camelcase
            ShipName,
            ShipAddress1,
            ShipZip,
            ShipCity,
            // eslint-disable-next-line camelcase
            FK_ShipState,
            // eslint-disable-next-line camelcase
            FK_ShipCountry,
            ConName,
            ConAddress1,
            ConZip,
            ConCity,
            // eslint-disable-next-line camelcase
            FK_ConState,
            // eslint-disable-next-line camelcase
            FK_ConCountry,
          } = item;

          return every(
            [
              ShipName,
              ShipAddress1,
              ShipZip,
              ShipCity,
              // eslint-disable-next-line camelcase
              FK_ShipState,
              // eslint-disable-next-line camelcase
              FK_ShipCountry,
              ConName,
              ConAddress1,
              ConZip,
              ConCity,
              // eslint-disable-next-line camelcase
              FK_ConState,
              // eslint-disable-next-line camelcase
              FK_ConCountry,
            ],
            (value) => !isEmpty(value) && value !== 'NULL'
          );
        })
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

async function queryDynamoDB(orderNo) {
  const params = {
    TableName: SHIPMENT_HEADER_TABLE,
    KeyConditionExpression: 'PK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };

  try {
    const result = await dynamoDb.query(params).promise();
    console.info('🚀 ~ file: index.js:344 ~ queryDynamoDB ~ result:', result);
    return result.Items;
  } catch (error) {
    console.error('Error querying DynamoDB:', error);
    throw error;
  }
}
