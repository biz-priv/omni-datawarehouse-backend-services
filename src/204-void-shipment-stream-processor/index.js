'use strict';

const AWS = require('aws-sdk');

const sns = new AWS.SNS();
const _ = require('lodash');

const dynamoDb = new AWS.DynamoDB.DocumentClient();
const { TYPES, VENDOR, STATUSES } = require('../shared/constants/204_create_shipment');
const { getUserEmail, sendSESEmail } = require('../shared/204-payload-generator/helper');
const { updateOrders } = require('../shared/204-payload-generator/apis');

const {
  STATUS_TABLE,
  CONSOLE_STATUS_TABLE,
  SHIPMENT_APAR_TABLE,
  SHIPMENT_HEADER_TABLE,
  SHIPMENT_APAR_INDEX_KEY_NAME,
  APAR_FAILURE_TABLE,
  LIVE_SNS_TOPIC_ARN,
  STAGE,
} = process.env;

let functionName;
module.exports.handler = async (event, context) => {
  functionName = _.get(context, 'functionName');
  console.info('🙂 -> file: index.js:8 -> functionName:', functionName);
  try {
    console.info('Event:', event);

    await Promise.all(event.Records.map(processRecord));
  } catch (error) {
    console.error('Error processing records:', error);
    throw error;
  }
};

async function processRecord(record) {
  let orderNo;
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
        orderNo = _.get(message, 'Keys.FK_OrderNo.S');
        console.info('🚀 ~ file: index.js:52 ~ processRecord ~ orderNo:', orderNo);
        const aparFailureDataArray = await fetchAparFailureData(orderNo);
        const highestObject = aparFailureDataArray.reduce((max, current) => {
          return current.FK_SeqNo > max.FK_SeqNo ? current : max;
        }, aparFailureDataArray[0]);
        const Note = _.get(highestObject, 'Note');
        console.info('note: ', Note);
        const shipmentAparData = await fetchShipmentAparData(orderNo);
        const FkServiceId = _.get(shipmentAparData, '[0].FK_ServiceId');
        console.info('FkServiceId: ', FkServiceId);
        if (shipmentAparData.length > 0 && shipmentAparData) {
          const Body = Object.keys(newImage).length ? newImage : oldImage;
          await processShipmentAparData({ orderId: orderNo, newImage: Body, note: Note });
        }
      }
    } else if (
      dynamoTableName === SHIPMENT_HEADER_TABLE &&
      _.get(newImage, 'FK_OrderStatusId') === 'CAN'
    ) {
      console.info('🚀 ~ file: index.js:45 ~ processRecord ~ dynamoTableName:', dynamoTableName);
      orderNo = newImage.PK_OrderNo;
      console.info('🚀 ~ file: index.js:46 ~ processRecord ~ orderId:', orderNo);
      const shipmentAparDataArray = await fetchOrderNos(orderNo);
      console.info(
        '🚀 ~ file: index.js:48 ~ processRecord ~ shipmentAparDataArray:',
        shipmentAparDataArray
      );
      const consolNo = Number(_.get(shipmentAparDataArray, '[0].ConsolNo'));
      if (shipmentAparDataArray.length > 0 && consolNo === 0) {
        // Grouping shipmentAparDataArray by FK_OrderNo
        const orderGroups = _.groupBy(shipmentAparDataArray, 'FK_OrderNo');
        console.info('🚀 ~ file: index.js:52 ~ processRecord ~ orderGroups:', orderGroups);

        // Processing each group
        await Promise.all(
          _.map(orderGroups, async (aparDataArray, orderId) => {
            // Process data for each orderId
            await Promise.all(
              aparDataArray.map(async (aparData) => {
                await processShipmentAparData({
                  orderId,
                  newImage: aparData,
                  note: 'Shipment got Cancelled',
                });
              })
            );
          })
        );
      } else {
        console.info(
          'No shipment apar data found for order IDs or the shipments are Consolidations.'
        );
        return;
      }
    }
  } catch (error) {
    console.error('Error processing record:', error);
    await publishSNSTopic({
      message: `${error.message}\n
                function name: `,
      stationCode: 'SUPPORT',
      subject: `An error occured while voiding the process ~ FK_OrderNo: ${orderNo}`,
    });
    throw error;
  }
}

const queryOrderStatusTable = async (orderNo) => {
  const orderParam = {
    TableName: STATUS_TABLE,
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': String(orderNo),
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
    console.info('🚀 ~ file: index.js:122 ~ queryConsolStatusTable ~ queryParams:', queryParams);
    const result = await dynamoDb.query(queryParams).promise();
    return _.get(result, 'Items', []);
  } catch (error) {
    console.error('Error querying console status table:', error);
    throw error;
  }
}

async function processShipmentAparData({ orderId, newImage, note }) {
  try {
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
    const consolidation = _.get(newImage, 'Consolidation');
    const seqNo = _.get(newImage, 'SeqNo');

    let userEmail;
    let stationCode;
    let type;

    if (consolNo === 0 && ['HS', 'TL'].includes(serviceLevelId)) {
      userEmail = await getUserEmail({ userId });
      const fetchedStation = await getControllingStation(orderId);
      if (fetchedStation !== '') {
        stationCode = fetchedStation;
      } else if (controlStation !== '') {
        stationCode = controlStation;
      } else {
        stationCode = 'SUPPORT';
      }
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

    if (type === TYPES.NON_CONSOLE) {
      const orderStatusResult = await queryOrderStatusTable(orderId);
      if (orderStatusResult.length > 0 && orderStatusResult[0].Status === STATUSES.SENT) {
        const { id, stops } = _.get(orderStatusResult, '[0].Response', {});
        const orderData = { __type: 'orders', company_id: 'TMS', id, status: 'V', stops };
        await updateOrders({ payload: orderData, orderId, consolNo });
        await updateStatusTables({ orderId, type });
        console.info('Voided the WT orderId:', orderId, '#PRO:', id);
      }
    }
    if (type === TYPES.CONSOLE) {
      const orderStatusResult = await queryOrderStatusTable(consolNo);
      const fetchedOrderData = await fetchOrderData({ consolNo });

      if (
        (orderStatusResult.length > 0 && orderStatusResult[0].Status === STATUSES.SENT) ||
        (orderStatusResult.length > 0 &&
          orderStatusResult[0].Status === STATUSES.SENT &&
          fetchedOrderData.length === 0)
      ) {
        const { id, stops } = _.get(orderStatusResult, '[0].Response', {});
        const orderData = { __type: 'orders', company_id: 'TMS', id, status: 'V', stops };
        await updateOrders({ payload: orderData, orderId, consolNo });
        await updateStatusTables({ orderId: consolNo, type });
        console.info('Voided the WT orderId:', orderId, '#PRO:', id);
      }
    }
    if (type === TYPES.MULTI_STOP) {
      const consolStatusResult = await queryConsolStatusTable(consolNo);
      console.info(
        '🚀 ~ file: index.js:183 ~ processShipmentAparData ~ consolStatusResult:',
        consolStatusResult
      );
      const fetchedOrderData = await fetchOrderData({ consolNo });

      if (
        (consolStatusResult.length > 0 &&
          consolStatusResult[0].Status === STATUSES.SENT &&
          seqNo === '9999' &&
          consolidation === 'Y') ||
        (consolStatusResult.length > 0 &&
          consolStatusResult[0].Status === STATUSES.SENT &&
          fetchedOrderData.length === 0)
      ) {
        const { id, stops } = _.get(consolStatusResult, '[0].Response', {});
        const orderData = { __type: 'orders', company_id: 'TMS', id, status: 'V', stops };
        await updateOrders({ payload: orderData, orderId, consolNo });
        await updateStatusTables({ consolNo, type });
        console.info('Voided the WT consolNo:', consolNo, '#PRO:', id);
      }
    }
    await sendSESEmail({
      functionName,
      message: `The shipment associated with the following details has been cancelled:\n
                Order ID: ${orderId}\n
                Consolidation Number: ${consolNo}\n
                Note: ${note}`,
      userEmail,
      subject: {
        Data: `PB VOID NOTIFICATION - ${STAGE} ~ FK_OrderNo: ${orderId} ~ ConsolNo: ${consolNo}`,
        Charset: 'UTF-8',
      },
    });
    await publishSNSTopic({
      message: `The shipment associated with the following details has been cancelled:\n
                Order ID: ${orderId}\n
                Consolidation Number: ${consolNo}\n
                Note: ${note}`,
      stationCode,
      subject: `PB VOID NOTIFICATION - ${STAGE} ~ FK_OrderNo: ${orderId} ~ ConsolNo: ${consolNo}`,
    });
  } catch (error) {
    console.error('Error in processShipmentAparData:', error);
    throw error;
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

async function fetchAparFailureData(orderId) {
  try {
    const params = {
      TableName: APAR_FAILURE_TABLE,
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

async function fetchShipmentAparData(orderId) {
  try {
    const params = {
      TableName: SHIPMENT_APAR_TABLE,
      KeyConditionExpression: 'FK_OrderNo = :orderNo',
      FilterExpression: 'FK_ServiceId = :FK_ServiceId',
      ExpressionAttributeValues: {
        ':orderNo': orderId,
        ':FK_ServiceId': 'SE',
      },
    };
    const data = await dynamoDb.query(params).promise();
    return _.get(data, 'Items', []);
  } catch (err) {
    console.error('Error shipment apar data:', err);
    throw err;
  }
}

async function getControllingStation(orderId) {
  try {
    const params = {
      TableName: SHIPMENT_HEADER_TABLE,
      KeyConditionExpression: 'PK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderId,
      },
    };
    const data = await dynamoDb.query(params).promise();
    return _.get(data, 'Items[0].ControllingStation', '');
  } catch (err) {
    console.error('Error fetching order data:', err);
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
        'FK_VendorId = :FK_VendorId and Consolidation = :Consolidation and SeqNo <> :SeqNo and FK_OrderNo <> :FK_OrderNo',
      ExpressionAttributeValues: {
        ':ConsolNo': String(consolNo),
        ':FK_VendorId': VENDOR,
        ':Consolidation': 'N',
        ':SeqNo': '9999',
        ':FK_OrderNo': String(consolNo),
      },
    };
    console.info('🙂 -> file: index.js:216 -> fetchOrderData -> params:', params);
    const data = await dynamoDb.query(params).promise();
    console.info('🙂 -> file: index.js:138 -> fetchItemFromTable -> data:', data);
    return _.get(data, 'Items', []);
  } catch (err) {
    console.error('🚀 ~ file: index.js:263 ~ fetchOrderData ~ err:', err);
    throw err;
  }
}

async function updateStatusTables({ consolNo, type, orderId }) {
  try {
    let updateParams;
    if (type === TYPES.MULTI_STOP) {
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
    } else if (type === TYPES.CONSOLE || type === TYPES.NON_CONSOLE) {
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

async function publishSNSTopic({ message, stationCode, subject }) {
  try {
    const params = {
      TopicArn: LIVE_SNS_TOPIC_ARN,
      Subject: subject,
      Message: `An error occurred in ${functionName}: \n${message}`,
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
