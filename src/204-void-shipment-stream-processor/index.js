'use strict';

const AWS = require('aws-sdk');

const sns = new AWS.SNS();
const _ = require('lodash');

const sqs = new AWS.SQS();
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const { TYPES, VENDOR, STATUSES } = require('../shared/constants/204_create_shipment');
const { getUserEmail, sendSESEmail } = require('../shared/204-payload-generator/helper');
const { updateOrders, getOrders } = require('../shared/204-payload-generator/apis');

const {
  STATUS_TABLE,
  CONSOLE_STATUS_TABLE,
  SHIPMENT_APAR_TABLE,
  SHIPMENT_HEADER_TABLE,
  SHIPMENT_APAR_INDEX_KEY_NAME,
  APAR_FAILURE_TABLE,
  LIVE_SNS_TOPIC_ARN,
  STAGE,
  QUEUE_URL,
} = process.env;

let functionName;
module.exports.handler = async (event, context) => {
  functionName = _.get(context, 'functionName');
  console.info('🙂 -> file: index.js:8 -> functionName:', functionName);
  try {
    console.info('Event:', event);

    await Promise.all(event.Records.map((record) => processRecord(record, event)));
  } catch (error) {
    console.error('Error processing records:', error);
    throw error;
  }
};

async function processRecord(record, event) {
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
      const fkVendorIdUpdated =
        oldImage.FK_VendorId === VENDOR &&
        newImage.FK_VendorId !== undefined &&
        newImage.FK_VendorId !== VENDOR;
      console.info(
        '🚀 ~ file: index.js:54 ~ processRecord ~ fkVendorIdUpdated:',
        fkVendorIdUpdated
      );
      const fkVendorIdDeleted =
        oldImage.FK_VendorId === VENDOR &&
        (!newImage.FK_VendorId || newImage.FK_VendorId === undefined);
      console.info(
        '🚀 ~ file: index.js:56 ~ processRecord ~ fkVendorIdDeleted:',
        fkVendorIdDeleted
      );

      if (fkVendorIdUpdated || fkVendorIdDeleted) {
        orderNo = _.get(message, 'Keys.FK_OrderNo.S');
        console.info('🚀 ~ file: index.js:52 ~ processRecord ~ orderNo:', orderNo);

        const Body = Object.keys(newImage).length ? newImage : oldImage;
        await processShipmentAparData({
          orderId: orderNo,
          newImage: Body,
          fkVendorIdDeleted,
        });
      }
    }
  } catch (error) {
    console.error('Error processing record:', error);
    try {
      const receiptHandle = _.get(event, 'Records[0].receiptHandle', '');

      console.info('receiptHandle:', receiptHandle);

      if (!receiptHandle) {
        throw new Error('No receipt handle found in the event');
      }

      // Delete the message from the SQS queue
      const deleteParams = {
        QueueUrl: QUEUE_URL,
        ReceiptHandle: receiptHandle,
      };

      await sqs.deleteMessage(deleteParams).promise();
      console.info('Message deleted successfully');
    } catch (e) {
      console.error('Error processing SQS event:', e);
    }
    await publishSNSTopic({
      message: `${error.message}\n
                function name: ${functionName}`,
      stationCode: 'SUPPORT',
      subject: `An error occured while voiding the process ~ FK_OrderNo: ${orderNo}`,
    });
    throw error;
  }
}

async function queryOrderStatusTable(orderNo) {
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
}

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

async function processShipmentAparData({ orderId, newImage, fkVendorIdDeleted = false }) {
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

    let shipmentHeaderResult;
    if (consolNo === 0 && ['HS', 'TL'].includes(serviceLevelId)) {
      userEmail = await getUserEmail({ userId });
      // Added delay for 45 secs to ensure the data in all the tables is populated.
      await setDelay(45);
      shipmentHeaderResult = await queryShipmentHeader({ orderId });

      const fetchedStation = _.get(shipmentHeaderResult, '[0].ControllingStation', '');
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
    } else {
      return;
    }
    let Note = '';
    let shipmentId;
    let housebill;
    if (type === TYPES.NON_CONSOLE) {
      const orderStatusResult = await queryOrderStatusTable(orderId);
      if (
        _.get(orderStatusResult, 'length', []) > 0 &&
        _.get(orderStatusResult, '[0].Status') === STATUSES.SENT
      ) {
        shipmentId = _.get(orderStatusResult, '[0].ShipmentId');
        console.info('🚀 ~ file: index.js:208 ~ shipmentId:', shipmentId);
        housebill = _.get(orderStatusResult, '[0].Housebill');
        console.info('🚀 ~ file: index.js:216 ~ housebill:', housebill);
        const fkOrderStatusId = _.get(shipmentHeaderResult, '[0].FK_OrderStatusId', '');
        console.info('🚀 ~ file: index.js:246 ~ fkOrderStatusId:', fkOrderStatusId);
        const brokerageStatusCheck = await checkBrokerageStatus(shipmentId);
        if (brokerageStatusCheck !== true) {
          console.info('Brokerage status is not one of OPEN, NEWAPI, NEWOMNI')
          await sendVoidNotificationEmail({ shipmentId, housebill, orderId, consolNo, userEmail, stage: STAGE })
          return;
        }

        if (fkVendorIdDeleted === true && fkOrderStatusId !== 'CAN') {
          const aparFailureDataArray = await fetchAparFailureData(orderId);
          const highestObject = aparFailureDataArray.reduce((max, current) => {
            return current.FK_SeqNo > max.FK_SeqNo ? current : max;
          }, aparFailureDataArray[0]);
          Note = _.get(highestObject, 'Note', '');
          console.info('note: ', Note);
          if (!Note) {
            throw new Error('No note found. Failed to cancel the shipment.');
          }
          const shipmentAparData = await fetchShipmentAparData(orderId);
          const FkServiceId = _.get(shipmentAparData, '[0].FK_ServiceId');
          console.info('FkServiceId: ', FkServiceId);
          if (!shipmentAparData.length) {
            console.info('Service exception is not found');
            throw new Error(`Service Exception is not found for FileNo: ${orderId}`);
          }
        } else if (fkVendorIdDeleted === true && fkOrderStatusId === 'CAN') {
          console.info('skipping the above conditions as it is cancelled by CAN milestone.');
        }
        const { id, stops } = _.get(orderStatusResult, '[0].Response', {});
        const orderData = { __type: 'orders', company_id: 'TMS', id, status: 'V', stops };
        await updateOrders({ payload: orderData, orderId, consolNo });
        await updateStatusTables({ orderId, type });
        console.info('Voided the WT orderId:', orderId, '#PRO:', id);
        await sendCancellationNotification(
          orderId,
          consolNo,
          userEmail,
          stationCode,
          shipmentId,
          housebill
        );
      }
    }
    if (type === TYPES.CONSOLE) {
      const orderStatusResult = await queryOrderStatusTable(consolNo);
      const fetchedOrderData = await fetchOrderData({ consolNo });

      if (
        (_.get(orderStatusResult, 'length', []) > 0 &&
          _.get(orderStatusResult, '[0].Status') === STATUSES.SENT &&
          String(consolNo) === String(orderId)) ||
        (_.get(orderStatusResult, 'length', []) &&
          _.get(orderStatusResult, '[0].Status') === STATUSES.SENT &&
          _.get(fetchedOrderData, 'length', []) === 0)
      ) {
        shipmentId = _.get(orderStatusResult, '[0].ShipmentId');
        console.info('🚀 ~ file: index.js:261 ~ shipmentId:', shipmentId);
        housebill = _.get(orderStatusResult, '[0].Housebill');
        console.info('🚀 ~ file: index.js:263 ~ housebill:', housebill);
        const brokerageStatusCheck = await checkBrokerageStatus(shipmentId);
        if (brokerageStatusCheck !== true) {
          console.info('Brokerage status is not one of OPEN, NEWAPI, NEWOMNI')
          await sendVoidNotificationEmail({ shipmentId, housebill, orderId, consolNo, userEmail, stage: STAGE })
          return;
        }
        const { id, stops } = _.get(orderStatusResult, '[0].Response', {});
        const orderData = { __type: 'orders', company_id: 'TMS', id, status: 'V', stops };
        await updateOrders({ payload: orderData, orderId, consolNo });
        await updateStatusTables({ orderId: consolNo, type });
        console.info('Voided the WT orderId:', orderId, '#PRO:', id);
        await sendCancellationNotification(
          orderId,
          consolNo,
          userEmail,
          stationCode,
          shipmentId,
          housebill
        );
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
        (_.get(consolStatusResult, 'length', []) > 0 &&
          _.get(consolStatusResult, '[0].Status') === STATUSES.SENT &&
          seqNo === '9999' &&
          consolidation === 'Y') ||
        (_.get(consolStatusResult, 'length', []) > 0 &&
          _.get(consolStatusResult, '[0].Status') === STATUSES.SENT &&
          _.get(fetchedOrderData, 'length', []) === 0)
      ) {
        shipmentId = _.get(consolStatusResult, '[0].ShipmentId');
        console.info('🚀 ~ file: index.js:288 ~ shipmentId:', shipmentId);
        const brokerageStatusCheck = await checkBrokerageStatus(shipmentId);
        if (brokerageStatusCheck !== true) {
          console.info('Brokerage status is not one of OPEN, NEWAPI, NEWOMNI')
          await sendVoidNotificationEmail({ shipmentId, housebill, orderId, consolNo, userEmail, stage: STAGE })
          return;
        }
        housebill = _.get(consolStatusResult, '[0].Housebill');
        console.info('🚀 ~ file: index.js:290 ~ housebill:', housebill);
        const { id, stops } = _.get(consolStatusResult, '[0].Response', {});
        const orderData = { __type: 'orders', company_id: 'TMS', id, status: 'V', stops };
        await updateOrders({ payload: orderData, orderId, consolNo });
        await updateStatusTables({ consolNo, type });
        console.info('Voided the WT consolNo:', consolNo, '#PRO:', id);
        await sendCancellationNotification(
          orderId,
          consolNo,
          userEmail,
          stationCode,
          shipmentId,
          housebill
        );
      }
    }
  } catch (error) {
    console.error('Error in processShipmentAparData:', error);
    throw error;
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

async function queryShipmentHeader({ orderId }) {
  try {
    const params = {
      TableName: SHIPMENT_HEADER_TABLE,
      KeyConditionExpression: 'PK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderId,
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

async function fetchOrderData({ consolNo }) {
  try {
    const params = {
      TableName: SHIPMENT_APAR_TABLE,
      IndexName: SHIPMENT_APAR_INDEX_KEY_NAME,
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      ProjectionExpression: 'FK_OrderNo',
      FilterExpression:
        'Consolidation = :Consolidation and SeqNo <> :SeqNo and FK_OrderNo <> :FK_OrderNo',
      ExpressionAttributeValues: {
        ':ConsolNo': String(consolNo),
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
    const mes = message.startsWith('\nDear Team,')
      ? message
      : `An error occurred in ${functionName}: ${message}`;

    const params = {
      TopicArn: LIVE_SNS_TOPIC_ARN,
      Subject: subject,
      Message: mes,
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

function setDelay(sec) {
  console.info('delay started');
  return new Promise((resolve, reject) => {
    if (typeof sec !== 'number' || sec < 0) {
      reject(new Error('Invalid time specified'));
    } else {
      setTimeout(() => {
        console.info('delay end');
        resolve(true);
      }, sec * 1000);
    }
  });
}

async function sendCancellationNotification(
  orderId,
  consolNo,
  userEmail,
  stationCode,
  shipmentId,
  housebill
) {
  const mess = `
  <!DOCTYPE html>
  <html>
  <head>
    <style>
      body {
        font-family: Arial, sans-serif;
      }
      .container {
        padding: 20px;
        border: 1px solid #ddd;
        border-radius: 5px;
        background-color: #f9f9f9;
      }
      .highlight {
        font-weight: bold;
      }
    </style>
  </head>
  <body>
    <div class="container">
      <p>Dear Team,</p>
      <p>The shipment associated with the following details has been cancelled:</p>
      <p><span class="highlight">#PRO:</span> <strong>${shipmentId}</strong><br>
        <span class="highlight">Housebill:</span> <strong>${housebill}</strong><br>
         <span class="highlight">File No:</span> <strong>${orderId}</strong><br>
         <span class="highlight">Consolidation Number:</span> <strong>${consolNo}</strong></p>
      <p>Thank you,<br>
      Omni Automation System</p>
      <p style="font-size: 0.9em; color: #888;">Note: This is a system generated email, Please do not reply to this email.</p>
    </div>
  </body>
  </html>
  `;

  const sub = `PB VOID NOTIFICATION - ${STAGE} ~ #PRO: ${shipmentId} | File No: ${orderId} | ConsolNo: ${consolNo}`;

  await sendSESEmail({
    functionName: 'sendSESEmail',
    message: mess,
    userEmail,
    subject: sub,
  });

  const message = `\nDear Team,\n
  The shipment associated with the following details has been cancelled:\n
  #PRO: ${shipmentId},
  Housebill: ${housebill},
  File No: ${orderId},
  Consolidation Number: ${consolNo}.\n
  Thank you,
  Omni Automation System\n
  Note: This is a system generated email, Please do not reply to this email.`;

  await publishSNSTopic({
    message,
    stationCode,
    subject: sub,
  });
}

// Common reusable function
async function checkBrokerageStatus(shipmentId, allowedStatuses = ['OPEN', 'NEWAPI', 'NEWOMNI']) {
  try {
    const orderResponse = await getOrders({ id: shipmentId });
    const movements = _.get(orderResponse, 'movements', []);
    const brokerageStatus = _.get(movements, '[0].brokerage_status', '');
    console.info('🚀 ~ file: index.js:575 ~ checkBrokerageStatus ~ brokerageStatus:', brokerageStatus)

    if (!_.includes(allowedStatuses, brokerageStatus)) {
      console.info(`Brokerage status is not in allowed statuses: ${allowedStatuses.join(', ')}`);
      return false;
    }
    return true;
  } catch (error) {
    console.error('Error checking brokerage status:', error);
    return false;
  }
}

// Function to generate the email message
function generateEmailMessage(shipmentId, housebill, orderId, consolNo) {
  return `
    <!DOCTYPE html>
    <html>
    <head>
      <style>
        body {
          font-family: Arial, sans-serif;
        }
        .container {
          padding: 20px;
          border: 1px solid #ddd;
          border-radius: 5px;
          background-color: #f9f9f9;
        }
        .highlight {
          font-weight: bold;
        }
      </style>
    </head>
    <body>
      <div class="container">
        <p>Dear Team,</p>
        <p>The shipment associated with the following details could not be cancelled as carrier is already assigned:</p>
        <p><span class="highlight">#PRO:</span> <strong>${shipmentId}</strong><br>
          <span class="highlight">Housebill:</span> <strong>${housebill}</strong><br>
          <span class="highlight">File No:</span> <strong>${orderId}</strong><br>
          <span class="highlight">Consolidation Number:</span> <strong>${consolNo}</strong></p>
        <p>Thank you,<br>
        Omni Automation System</p>
        <p style="font-size: 0.9em; color: #888;">Note: This is a system generated email, Please do not reply to this email.</p>
      </div>
    </body>
    </html>
  `;
}

// Function to generate the email subject
function generateEmailSubject(stage, shipmentId, orderId, consolNo) {
  return `PB VOID NOTIFICATION - ${stage} ~ #PRO: ${shipmentId} | File No: ${orderId} | ConsolNo: ${consolNo}`;
}

// Common function to send email
async function sendVoidNotificationEmail({ shipmentId, housebill, orderId, consolNo, userEmail, stage }) {
  const message = generateEmailMessage(shipmentId, housebill, orderId, consolNo);
  const subject = generateEmailSubject(stage, shipmentId, orderId, consolNo);

  await sendSESEmail({
    functionName: 'sendSESEmail',
    message,
    userEmail,
    subject,
  });
}
