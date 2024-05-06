'use strict';

const AWS = require('aws-sdk');
const _ = require('lodash');

const dynamoDb = new AWS.DynamoDB.DocumentClient();
const { TYPES, VENDOR, STATUSES } = require('../shared/constants/204_create_shipment');
const { getUserEmail } = require('../shared/204-payload-generator/helper');
const { updateOrders } = require('../shared/204-payload-generator/apis');

const { STATUS_TABLE, CONSOLE_STATUS_TABLE, SHIPMENT_APAR_TABLE, SHIPMENT_HEADER_TABLE } =
  process.env;

module.exports.handler = async (event) => {
  try {
    console.info('Event:', event);

    await Promise.all(event.Records.map(processRecord));
  } catch (error) {
    console.error('Error processing records:', error);
    throw error;
  }
};

async function processRecord(record) {
  try {
    const body = JSON.parse(_.get(record, 'body', ''));
    const message = JSON.parse(_.get(body, 'Message', ''));
    const newImage = AWS.DynamoDB.Converter.unmarshall(_.get(message, 'NewImage', {}));
    const oldImage = AWS.DynamoDB.Converter.unmarshall(_.get(message, 'OldImage', {}));
    const dynamoTableName = _.get(message, 'dynamoTableName', '');

    console.info('🚀 ~ New Image:', newImage);
    console.info('🚀 ~ Old Image:', oldImage);
    console.info('🚀 ~ Dynamo Table Name:', dynamoTableName);

    if (dynamoTableName === SHIPMENT_APAR_TABLE) {
      console.info('🚀 ~ file: index.js:38 ~ processRecord ~ dynamoTableName:', dynamoTableName);
      const fkVendorIdUpdated = oldImage.FK_VendorId === VENDOR && newImage.FK_VendorId !== VENDOR;
      console.info(
        '🚀 ~ file: index.js:40 ~ processRecord ~ fkVendorIdUpdated:',
        fkVendorIdUpdated
      );
      const fkVendorIdDeleted = oldImage.FK_VendorId === VENDOR && !newImage.FK_VendorId;
      console.info(
        '🚀 ~ file: index.js:42 ~ processRecord ~ fkVendorIdDeleted:',
        fkVendorIdDeleted
      );

      if (fkVendorIdUpdated || fkVendorIdDeleted) {
        const orderNo = _.get(message, 'Keys.FK_OrderNo.S');
        console.info('🚀 ~ file: index.js:52 ~ processRecord ~ orderNo:', orderNo);
        const Body = Object.keys(newImage).length ? newImage : oldImage;
        await processShipmentAparData({ orderId: orderNo, newImage: Body });
      }
    } else if (dynamoTableName === SHIPMENT_HEADER_TABLE) {
      console.info('🚀 ~ file: index.js:45 ~ processRecord ~ dynamoTableName:', dynamoTableName);
      const orderNo = newImage.PK_OrderNo;
      console.info('🚀 ~ file: index.js:46 ~ processRecord ~ orderId:', orderNo);
      const shipmentAparDataArray = await fetchOrderNos(orderNo);
      console.info(
        '🚀 ~ file: index.js:48 ~ processRecord ~ shipmentAparDataArray:',
        shipmentAparDataArray
      );

      if ((shipmentAparDataArray.length > 0) && shipmentAparDataArray[0].ConsolNo === 0) {
        // Grouping shipmentAparDataArray by FK_OrderNo
        const orderGroups = _.groupBy(shipmentAparDataArray, 'FK_OrderNo');
        console.info('🚀 ~ file: index.js:52 ~ processRecord ~ orderGroups:', orderGroups);

        // Processing each group
        await Promise.all(
          _.map(orderGroups, async (aparDataArray, orderId) => {
            // Process data for each orderId
            await Promise.all(
              aparDataArray.map(async (aparData) => {
                await processShipmentAparData({ orderId, newImage: aparData });
              })
            );
          })
        );
      } else {
        console.info('No shipment apar data found for order IDs.');
      }
    }
  } catch (error) {
    console.error('Error processing record:', error);
    throw error;
  }
}

const queryOrderStatusTable = async (orderNo) => {
  const orderParam = {
    TableName: STATUS_TABLE,
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };
  try {
    const result = await dynamoDb.query(orderParam).promise();
    return _.get(result, 'Items', []);
  } catch (error) {
    console.error('Error querying status table:', error);
    throw error;
  }
};

async function queryConsolStatusTable(consolNo) {
  try {
    const queryParams = {
      TableName: CONSOLE_STATUS_TABLE,
      KeyConditionExpression: 'ConsolNo = :consolNo',
      ExpressionAttributeValues: {
        ':consolNo': String(consolNo),
      },
    };
    const result = await dynamoDb.query(queryParams).promise();
    return _.get(result, 'Items', []);
  } catch (error) {
    console.error('Error querying console status table:', error);
    throw error;
  }
}

async function processShipmentAparData({ orderId, newImage }) {
  const consolNo = parseInt(_.get(newImage, 'ConsolNo', null), 10);
  console.info('🚀 ~ file: index.js:117 ~ processShipmentAparData ~ consolNo:', consolNo);
  const serviceLevelId = _.get(newImage, 'FK_ServiceId');
  console.info(
    '🚀 ~ file: index.js:119 ~ processShipmentAparData ~ serviceLevelId:',
    serviceLevelId
  );
  const vendorId = _.get(newImage, 'FK_VendorId', '').toUpperCase();
  console.info('🚀 ~ file: index.js:121 ~ processShipmentAparData ~ vendorId:', vendorId);
  const controlStation = _.get(newImage, 'FK_ConsolStationId');
  console.info(
    '🚀 ~ file: index.js:123 ~ processShipmentAparData ~ controlStation:',
    controlStation
  );
  const userId = _.get(newImage, 'UpdatedBy');
  console.info('🚀 ~ file: index.js:125 ~ processShipmentAparData ~ userId:', userId);

  let userEmail;
  let stationCode;
  let type;

  if (consolNo === 0 && ['HS', 'TL'].includes(serviceLevelId)) {
    userEmail = await getUserEmail({ userId });
    stationCode = controlStation === '' ? 'SUPPORT' : controlStation;
    type = TYPES.NON_CONSOLE;
  } else if (consolNo > 0 && ['HS', 'TL'].includes(serviceLevelId)) {
    userEmail = await getUserEmail({ userId });
    stationCode = controlStation === '' ? 'SUPPORT' : controlStation;
    type = TYPES.CONSOLE;
  } else if (!isNaN(consolNo) && consolNo !== null && serviceLevelId === 'MT') {
    userEmail = await getUserEmail({ userId });
    console.info('🚀 ~ file: index.js:105 ~ processShipmentAparData ~ userEmail:', userEmail);
    stationCode = controlStation === '' ? 'SUPPORT' : controlStation;
    console.info('🚀 ~ file: index.js:107 ~ processShipmentAparData ~ stationCode:', stationCode);
    type = TYPES.MULTI_STOP;
  }

  if (type === TYPES.CONSOLE || type === TYPES.NON_CONSOLE) {
    const orderStatusResult = await queryOrderStatusTable(orderId);
    if (
      orderStatusResult.length > 0 &&
      orderStatusResult[0].FK_OrderNo === orderId &&
      orderStatusResult[0].Status === STATUSES.SENT
    ) {
      const { id, stops } = _.get(orderStatusResult, '[0].Response', {});
      const orderData = { __type: 'orders', company_id: 'TMS', id, status: 'V', stops };
      await updateOrders({ payload: orderData, orderId, consolNo });
      await updateStatusTables({ consolNo, tableName: STATUS_TABLE, orderId });
      console.info('Voided the WT orderId:', orderId, '#PRO:', id);
    }
  } else {
    const consolStatusResult = await queryConsolStatusTable(consolNo);
    if (
      consolStatusResult.length > 0 &&
      consolStatusResult[0].ConsolNo === consolNo &&
      consolStatusResult[0].Status === STATUSES.SENT
    ) {
      const { id, stops } = _.get(consolStatusResult, '[0].Response', {});
      const orderData = { __type: 'orders', company_id: 'TMS', id, status: 'V', stops };
      await updateOrders({ payload: orderData, orderId, consolNo });
      await updateStatusTables({ consolNo, tableName: CONSOLE_STATUS_TABLE });
      console.info('Voided the WT consolNo:', consolNo, '#PRO:', id);
    }
  }
}

async function fetchOrderNos(orderId) {
  try {
    const params = {
      TableName: SHIPMENT_APAR_TABLE,
      KeyConditionExpression: 'FK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderId,
      },
    };
    const data = await dynamoDb.query(params).promise();
    return _.get(data, 'Items', []);
  } catch (err) {
    console.error('Error fetching order data:', err);
    throw err;
  }
}

async function updateStatusTables({ consolNo, tableName, orderId }) {
  try {
    let updateParams;
    if (tableName === CONSOLE_STATUS_TABLE) {
      updateParams = {
        TableName: CONSOLE_STATUS_TABLE,
        Key: { ConsolNo: String(consolNo) },
        UpdateExpression: 'set #Status = :status, ResetCount = :resetCount',
        ExpressionAttributeNames: { '#Status': 'Status' },
        ExpressionAttributeValues: {
          ':status': STATUSES.CANCELED,
          ':resetCount': 0,
        },
      };
    } else if (tableName === STATUS_TABLE) {
      updateParams = {
        TableName: STATUS_TABLE,
        Key: { FK_OrderNo: String(orderId) },
        UpdateExpression: 'set #Status = :status, ResetCount = :resetCount',
        ExpressionAttributeNames: { '#Status': 'Status' },
        ExpressionAttributeValues: {
          ':status': STATUSES.CANCELED,
          ':resetCount': 0,
        },
      };
    } else {
      throw new Error('Invalid table name provided');
    }

    console.info('Update status table parameters:', updateParams);
    await dynamoDb.update(updateParams).promise();
    return 'success';
  } catch (error) {
    console.error('Error updating status table:', error);
    throw error;
  }
}
