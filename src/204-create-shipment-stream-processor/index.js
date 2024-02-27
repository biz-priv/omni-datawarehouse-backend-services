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

const {
  STATUS_TABLE,
  SNS_TOPIC_ARN,
  STAGE,
  CONSOLE_STATUS_TABLE,
  SHIPMENT_APAR_INDEX_KEY_NAME,
  SHIPMENT_APAR_TABLE,
  SHIPMENT_HEADER_TABLE,
  ACCEPTED_BILLNOS,
} = process.env;
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

let functionName;
let orderId;
module.exports.handler = async (event, context) => {
  console.info('🚀 ~ file: index.js:26 ~ event:', JSON.stringify(event));
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

        const consolidation = get(shipmentAparData, 'Consolidation');

        const controlStation = get(shipmentAparData, 'FK_ConsolStationId');

        if (consolNo === 0 && includes(['HS', 'TL'], serviceLevelId) && vendorId === VENDOR) {
          console.info('Non Console');
          type = TYPES.NON_CONSOLE;
          const shipmentHeaderResult = await fetchBillNos({ orderNo: orderId });
          // Check if there are any items in the result
          if (shipmentHeaderResult.length === 0) {
            console.info('No shipment header data found. Skipping process.');
            return true; // Skip further processing
          }

          // Extract bill numbers from the result
          const billNumbers = shipmentHeaderResult.map((item) => item.BillNo);
          console.info('🚀 ~ file: index.js:65 ~ get ~ billNumbers:', billNumbers);

          // Check if any of the bill numbers are in the accepted list
          const commonBillNos = ACCEPTED_BILLNOS.split(',').filter((billNo) =>
            billNumbers.includes(billNo)
          );

          // If there are no common bill numbers, skip further processing
          if (commonBillNos.length === 0) {
            await publishSNSTopic({
              message: `BillNo does not include any of the accepted bill numbers. Skipping process for this shipment.\nBill numbers we received: ${billNumbers}.`,
            });
            console.info(
              'Ignoring shipment, BillNo does not include any of the accepted bill numbers. Skipping process.'
            );
            return true;
          }
        }

        if (
          consolNo > 0 &&
          consolidation === 'Y' &&
          includes(['HS', 'TL'], serviceLevelId) &&
          vendorId === VENDOR &&
          controlStation === 'OTR'
        ) {
          console.info('Console');
          type = TYPES.CONSOLE;
        }

        if (
          !isNaN(consolNo) &&
          consolNo !== null &&
          consolidation === 'N' &&
          serviceLevelId === 'MT' &&
          controlStation === 'OTR'
        ) {
          console.info('Multi Stop');
          type = TYPES.MULTI_STOP;
          const existingConsol = await fetchConsoleStatusTable({ consolNo });
          if (Object.keys(existingConsol).length > 0) {
            await updateConsolStatusTable({
              consolNo,
              resetCount: get(existingConsol, 'ResetCount', 0),
            });
          } else {
            await insertConsoleStatusTable({ consolNo, status: STATUSES.PENDING });
          }
        }

        if (type) {
          return await checkAndUpdateOrderTable({ orderNo: orderId, type, shipmentAparData });
        }
        return true;
      })
    );

    return `Successfully processed ${get(event, 'Records', []).length} records.`;
  } catch (e) {
    console.error(
      '🚀 ~ file: shipment_apar_table_stream_processor.js:117 ~ module.exports.handler= ~ e:',
      e
    );
    return await publishSNSTopic({
      message: `Error processing order id: ${orderId}, ${e.message}. \n Please retrigger the process by changing any field in omni-wt-rt-shipment-apar-${STAGE} after fixing the error.`,
    });
  }
};

async function insertShipmentStatus({ orderNo, status, type, tableStatuses, shipmentAparData }) {
  try {
    const params = {
      TableName: STATUS_TABLE,
      Item: {
        FK_OrderNo: orderNo,
        Status: status,
        Type: type,
        TableStatuses: tableStatuses,
        ShipmentAparData: shipmentAparData,
        CreatedAt: moment.tz('America/Chicago').format(),
        LastUpdateBy: functionName,
        LastUpdatedAt: moment.tz('America/Chicago').format(),
      },
    };
    console.info('🙂 -> file: index.js:106 -> params:', params);
    await dynamoDb.put(params).promise();
    console.info('Record created successfully.');
  } catch (error) {
    console.error('🚀 ~ file: index.js:121 ~ insertShipmentStatus ~ error:', error);
    throw error;
  }
}

async function publishSNSTopic({ message }) {
  await sns
    .publish({
      TopicArn: SNS_TOPIC_ARN,
      Subject: `POWERBROKER ERROR NOTIFIACATION - ${STAGE}}`,
      Message: `An error occurred in ${functionName}: ${message}`,
    })
    .promise();
}

async function checkAndUpdateOrderTable({ orderNo, type, shipmentAparData }) {
  const existingOrderParam = {
    TableName: STATUS_TABLE,
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };
  console.info('🚀 ~ file: index.js:144 ~ existingOrderParam:', existingOrderParam);
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
      shipmentAparData,
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
    let orderData = await fetchOrderData({ consolNo });
    orderData = orderData.map((data) => get(data, 'FK_OrderNo'));
    console.info('🙂 -> file: index.js:159 -> insertConsoleStatusTable -> orderData:', orderData);
    const TableStatuses = {};
    orderData.map((singleOrderId) => {
      TableStatuses[singleOrderId] = CONSOLE_WISE_TABLES[TYPES.MULTI_STOP];
      return true;
    });
    console.info(
      '🙂 -> file: index.js:163 -> insertConsoleStatusTable -> TableStatuses:',
      TableStatuses
    );
    const params = {
      TableName: CONSOLE_STATUS_TABLE,
      Item: {
        ConsolNo: String(consolNo),
        Status: status,
        TableStatuses,
        CreatedAt: moment.tz('America/Chicago').format(),
        LastUpdateBy: functionName,
        LastUpdatedAt: moment.tz('America/Chicago').format(),
      },
    };
    console.info('🙂 -> file: index.js:106 -> params:', params);
    await dynamoDb.put(params).promise();
    console.info('Record created successfully.');
  } catch (error) {
    console.error('🚀 ~ file: index.js:202 ~ insertConsoleStatusTable ~ error:', error);
    throw error;
  }
}

async function updateStatusTable({ orderNo, resetCount }) {
  try {
    const updateParam = {
      TableName: STATUS_TABLE,
      Key: { FK_OrderNo: orderNo },
      UpdateExpression:
        'set ResetCount = :resetCount, LastUpdateBy = :lastUpdateBy, LastUpdatedAt = :lastUpdatedAt',
      ExpressionAttributeValues: {
        ':resetCount': resetCount + 1,
        ':lastUpdateBy': functionName,
        ':lastUpdatedAt': moment.tz('America/Chicago').format(),
      },
    };
    console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
    return await dynamoDb.update(updateParam).promise();
  } catch (error) {
    console.error('🚀 ~ file: index.js:223 ~ updateStatusTable ~ error:', error);
    throw error;
  }
}

async function fetchItemFromTable({ params }) {
  try {
    console.info('🙂 -> file: index.js:135 -> params:', params);
    const data = await dynamoDb.query(params).promise();
    console.info('🙂 -> file: index.js:138 -> fetchItemFromTable -> data:', data);
    return get(data, 'Items.[0]', {});
  } catch (err) {
    console.error('🚀 ~ file: index.js:235 ~ fetchItemFromTable ~ err:', err);
    throw err;
  }
}

async function fetchOrderData({ consolNo }) {
  try {
    const params = {
      TableName: SHIPMENT_APAR_TABLE,
      IndexName: SHIPMENT_APAR_INDEX_KEY_NAME,
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      ProjectionExpression: 'FK_OrderNo',
      FilterExpression:
        'FK_VendorId = :FK_VendorId and Consolidation = :Consolidation and FK_ServiceId = :FK_ServiceId and SeqNo <> :SeqNo and FK_OrderNo <> :FK_OrderNo',
      ExpressionAttributeValues: {
        ':ConsolNo': String(consolNo),
        ':FK_VendorId': VENDOR,
        ':Consolidation': 'N',
        ':FK_ServiceId': 'MT',
        ':SeqNo': '9999',
        ':FK_OrderNo': String(consolNo),
      },
    };
    console.info('🙂 -> file: index.js:216 -> fetchOrderData -> params:', params);
    const data = await dynamoDb.query(params).promise();
    console.info('🙂 -> file: index.js:138 -> fetchItemFromTable -> data:', data);
    return get(data, 'Items', []);
  } catch (err) {
    console.error('🚀 ~ file: index.js:263 ~ fetchOrderData ~ err:', err);
    throw err;
  }
}

async function fetchBillNos({ orderNo }) {
  try {
    const params = {
      TableName: SHIPMENT_HEADER_TABLE,
      KeyConditionExpression: 'PK_OrderNo = :orderNo',
      FilterExpression: 'ShipQuote = :shipquote',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
        ':shipquote': 'S',
      },
    };
    console.info('🙂 -> file: index.js:216 -> fetchOrderData -> params:', params);
    const data = await dynamoDb.query(params).promise();
    console.info('🙂 -> file: index.js:138 -> fetchItemFromTable -> data:', data);
    return get(data, 'Items', []);
  } catch (err) {
    console.error('🚀 ~ file: index.js:263 ~ fetchOrderData ~ err:', err);
    throw err;
  }
}

async function fetchConsoleStatusTable({ consolNo }) {
  try {
    const existingOrderParam = {
      TableName: CONSOLE_STATUS_TABLE,
      KeyConditionExpression: 'ConsolNo = :consolNo',
      FilterExpression: '#Status = :status',
      ExpressionAttributeNames: { '#Status': 'Status' },
      ExpressionAttributeValues: {
        ':consolNo': String(consolNo),
        ':status': STATUSES.SENT,
      },
    };

    return await fetchItemFromTable({ params: existingOrderParam });
  } catch (error) {
    console.error('🚀 ~ file: index.js:283 ~ fetchConsoleStatusTable ~ error:', error);
    throw error;
  }
}

async function updateConsolStatusTable({ consolNo, resetCount }) {
  try {
    const updateParam = {
      TableName: CONSOLE_STATUS_TABLE,
      Key: { ConsolNo: String(consolNo) },
      UpdateExpression:
        'set ResetCount = :resetCount, LastUpdateBy = :lastUpdateBy, LastUpdatedAt = :lastUpdatedAt',
      ExpressionAttributeValues: {
        ':resetCount': resetCount + 1,
        ':lastUpdateBy': functionName,
        ':lastUpdatedAt': moment.tz('America/Chicago').format(),
      },
    };
    console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
    return await dynamoDb.update(updateParam).promise();
  } catch (error) {
    console.error('🚀 ~ file: index.js:304 ~ updateConsolStatusTable ~ error:', error);
    throw error;
  }
}
