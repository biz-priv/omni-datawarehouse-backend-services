'use strict';
const { get, includes } = require('lodash');
const AWS = require('aws-sdk');
const {
  STATUSES,
  TYPES,
  CONSOLE_WISE_TABLES,
  VENDOR,
} = require('../shared/constants/204_create_shipment');
const moment = require('moment-timezone');

const { STATUS_TABLE, SNS_TOPIC_ARN, STAGE, CONSOLE_STATUS_TABLE } = process.env;
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

let functionName;
let orderId;
module.exports.handler = async (event, context) => {
  console.info('🚀 ~ file: index.js:26 ~ event:', event);
  try {
    functionName = get(context, 'functionName');
    let type;
    await Promise.all(
      get(event, 'Records', []).map(async (record) => {
        const shipmentAparData = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
        console.info('🙂 -> file: index.js:17 -> shipmentAparData:', shipmentAparData);

        orderId = get(shipmentAparData, 'FK_OrderNo');
        console.info('🙂 -> file: index.js:23 -> orderId:', orderId);

        const consolNo = parseInt(get(shipmentAparData, 'ConsolNo', null), 10);
        console.info('🙂 -> file: index.js:23 -> consolNo:', consolNo);

        const serviceLevelId = get(shipmentAparData, 'FK_ServiceId');
        console.info('🙂 -> file: index.js:26 -> serviceLevelId:', serviceLevelId);

        const vendorId = get(shipmentAparData, 'FK_VendorId', '').toUpperCase();
        console.info('🙂 -> file: index.js:33 -> vendorId:', vendorId);

        if (consolNo === 0 && includes(['HS', 'TL'], serviceLevelId) && vendorId === VENDOR) {
          console.info('Non Console');
          type = TYPES.NON_CONSOLE;
        }

        if (
          consolNo > 0 &&
          get(shipmentAparData, 'Consolidation') === 'Y' &&
          includes(['HS', 'TL'], serviceLevelId) &&
          vendorId === VENDOR
        ) {
          console.info('Console');
          type = TYPES.CONSOLE;
        }

        if (
          !isNaN(consolNo) &&
          consolNo !== null &&
          get(shipmentAparData, 'Consolidation') === 'N' &&
          serviceLevelId === 'MT'
        ) {
          console.info('Multi Stop');
          type = TYPES.MULTI_STOP;
          await insertConsoleStatusTable({ consolNo, status: STATUSES.PENDING });
        }

        if (type) {
          return await checkAndUpdateOrderTable({ orderNo: orderId, type });
        }
        return true;
      })
    );

    return `Successfully processed ${get(event, 'Records', []).length} records.`;
  } catch (e) {
    console.error(
      '🚀 ~ file: shipment_header_table_stream_processor.js:117 ~ module.exports.handler= ~ e:',
      e
    );
    return await publishSNSTopic({
      message: `Error processing order id: ${orderId}, ${e.message}. \n Please retrigger the process by changing any field in omni-wt-rt-shipment-apar-${STAGE} after fixing the error.`,
    });
  }
};

async function insertShipmentStatus({ orderNo, status, type, tableStatuses }) {
  try {
    const params = {
      TableName: STATUS_TABLE,
      Item: {
        FK_OrderNo: orderNo,
        Status: status,
        Type: type,
        TableStatuses: tableStatuses,
        CreatedAt: moment.tz('America/Chicago').format(),
        LastUpdateBy: functionName,
        LastUpdatedAt: moment.tz('America/Chicago').format(),
      },
    };
    console.info('🙂 -> file: index.js:106 -> params:', params);
    await dynamoDb.put(params).promise();
    console.info('Record created successfully.');
  } catch (error) {
    console.info('🙂 -> file: index.js:78 -> error:', error);
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

async function checkAndUpdateOrderTable({ orderNo, type }) {
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

  if (
    Object.keys(existingOrder).length <= 0 ||
    (Object.keys(existingOrder).length > 0 && get(existingOrder, 'Status') === STATUSES.PENDING)
  ) {
    return await insertShipmentStatus({
      orderNo: orderId,
      status: STATUSES.PENDING,
      type,
      tableStatuses: CONSOLE_WISE_TABLES[type],
    });
  }

  if (Object.keys(existingOrder).length > 0 && get(existingOrder, 'Status') !== STATUSES.PENDING) {
    return await updateStatusTable({
      orderNo: orderId,
      resetCount: get(existingOrder, 'ResetCount', 0),
    });
  }
  return true;
}

async function insertConsoleStatusTable({ consolNo, status }) {
  try {
    const params = {
      TableName: CONSOLE_STATUS_TABLE,
      Item: {
        ConsolNo: String(consolNo),
        Status: status,
        CreatedAt: moment.tz('America/Chicago').format(),
        LastUpdateBy: functionName,
        LastUpdatedAt: moment.tz('America/Chicago').format(),
      },
    };
    console.info('🙂 -> file: index.js:106 -> params:', params);
    await dynamoDb.put(params).promise();
    console.info('Record created successfully.');
  } catch (error) {
    console.info('🙂 -> file: index.js:78 -> error:', error);
    throw error;
  }
}

async function updateStatusTable({ orderNo, resetCount }) {
  const updateParam = {
    TableName: STATUS_TABLE,
    Key: { FK_OrderNo: orderNo },
    UpdateExpression: 'set ResetCount = :resetCount',
    ExpressionAttributeValues: {
      ':resetCount': resetCount + 1,
    },
  };
  console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
  return await dynamoDb.update(updateParam).promise();
}

async function fetchItemFromTable({ params }) {
  try {
    console.info('🙂 -> file: index.js:135 -> params:', params);
    const data = await dynamoDb.query(params).promise();
    console.info('🙂 -> file: index.js:138 -> fetchItemFromTable -> data:', data);
    return get(data, 'Items.[0]', {});
  } catch (err) {
    console.info('🙂 -> file: index.js:177 -> fetchItemFromTable -> err:', err);
    throw err;
  }
}
