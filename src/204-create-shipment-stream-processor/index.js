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

const { STATUS_TABLE, SNS_TOPIC_ARN, STAGE } = process.env;
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

let functionName;
let orderId;
module.exports.handler = async (event, context) => {
  try {
    console.info('🙂 -> file: index.js:11 -> SNS_TOPIC_ARN:', SNS_TOPIC_ARN);

    functionName = get(context, 'functionName');
    console.info('🙂 -> file: index.js:8 -> functionName:', functionName);
    console.info(
      '🚀 ~ file: shipment_header_table_stream_processor.js:8 ~ exports.handler= ~ event:',
      JSON.stringify(event)
    );

    for (const record of get(event, 'Records', [])) {
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
        return await insertShipmentStatus({
          orderNo: orderId,
          status: STATUSES.PENDING,
          type: TYPES.NON_CONSOLE,
          tableStatuses: CONSOLE_WISE_TABLES[TYPES.NON_CONSOLE],
          ShipmentAparData: shipmentAparData,
        });
      }

      if (
        consolNo > 0 &&
        get(shipmentAparData, 'Consolidation') === 'Y' &&
        includes(['HS', 'TL'], serviceLevelId) &&
        vendorId === VENDOR
      ) {
        console.info('Console');
        return await insertShipmentStatus({
          orderNo: orderId,
          status: STATUSES.PENDING,
          type: TYPES.CONSOLE,
          tableStatuses: CONSOLE_WISE_TABLES[TYPES.CONSOLE],
          ShipmentAparData: shipmentAparData,
        });
      }

      if (
        !isNaN(consolNo) &&
        consolNo >= 0 &&
        get(shipmentAparData, 'Consolidation') === 'N' &&
        includes(['MT'], serviceLevelId) &&
        vendorId === VENDOR
      ) {
        console.info('Multi Stops');
        const result = await fetchConsoleForOrderId({ consolNo });
        console.info('🙂 -> file: index.js:81 -> result:', result);
        const tableStatuses = {};
        get(result, 'Items', []).map(
          async (item) =>
            (tableStatuses[get(item, 'FK_OrderNo')] = CONSOLE_WISE_TABLES[TYPES.MULTI_STOP])
        );

        await insertShipmentStatus({
          orderNo: orderId,
          status: STATUSES.PENDING,
          type: TYPES.MULTI_STOP,
          tableStatuses,
          ShipmentAparData: shipmentAparData,
        });
        return true;
      }
    }

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

async function insertShipmentStatus({ orderNo, status, type, tableStatuses, ShipmentAparData }) {
  try {
    const params = {
      TableName: STATUS_TABLE,
      Item: {
        FK_OrderNo: orderNo,
        Status: status,
        Type: type,
        TableStatuses: tableStatuses,
        ShipmentAparData,
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

async function fetchConsoleForOrderId({ consolNo }) {
  const params = {
    TableName: 'omni-wt-rt-shipment-apar-dev',
    IndexName: 'omni-ivia-ConsolNo-index-dev',
    KeyConditionExpression: 'ConsolNo = :ConsolNo',
    FilterExpression:
      'FK_VendorId = :FK_VendorId and Consolidation = :Consolidation and FK_ServiceId = :FK_ServiceId and SeqNo <> :SeqNo and FK_OrderNo <> :FK_OrderNo',
    ExpressionAttributeValues: {
      ':ConsolNo': consolNo.toString(),
      ':FK_VendorId': 'LIVELOGI',
      ':Consolidation': 'N',
      ':FK_ServiceId': 'MT',
      ':SeqNo': '9999',
      ':FK_OrderNo': consolNo.toString(),
    },
  };
  return await dynamoDb.query(params).promise();
}
