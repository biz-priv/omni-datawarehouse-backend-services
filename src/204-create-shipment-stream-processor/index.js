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
  console.info('🚀 ~ file: index.js:26 ~ event:', event);
  try {
    functionName = get(context, 'functionName');
    /**
     * added 45 sec delay
     */
    await setDelay(45);

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
        await insertShipmentStatus({
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
        await insertShipmentStatus({
          orderNo: orderId,
          status: STATUSES.PENDING,
          type: TYPES.CONSOLE,
          tableStatuses: CONSOLE_WISE_TABLES[TYPES.CONSOLE],
          ShipmentAparData: shipmentAparData,
        });
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

async function insertShipmentStatus({
  orderNo,
  consolNo,
  status,
  type,
  tableStatuses,
  ShipmentAparData,
}) {
  try {
    const params = {
      TableName: STATUS_TABLE,
      Item: {
        FK_OrderNo: orderNo,
        ConsolNo: consolNo,
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

/**
 * creates delay of {sec}
 * @param {*} sec
 * @returns
 */
function setDelay(sec) {
  console.info('delay started');
  return new Promise((resolve) => {
    setTimeout(() => {
      console.info('delay end');
      resolve(true);
    }, sec * 1000);
  });
}
