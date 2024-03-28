'use strict';
const AWS = require('aws-sdk');
const _ = require('lodash');
const moment = require('moment-timezone');
const { v4: uuidv4 } = require('uuid');

const { mtPayload } = require('../shared/204-payload-generator/payloads');
const {
  fetchDataFromTablesList,
  getUserEmail,
  sendSESEmail,
} = require('../shared/204-payload-generator/helper');
const {
  sendPayload,
  updateOrders,
  liveSendUpdate,
} = require('../shared/204-payload-generator/apis');
const { STATUSES, TYPES } = require('../shared/constants/204_create_shipment');

const { LIVE_SNS_TOPIC_ARN, CONSOL_STATUS_TABLE, OUTPUT_TABLE, STAGE, STATUS_TABLE } = process.env;
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

let functionName;
const type = TYPES.MULTI_STOP;
module.exports.handler = async (event, context) => {
  console.info(
    '🙂 -> file: index.js:8 -> module.exports.handler= -> event:',
    JSON.stringify(event)
  );
  functionName = _.get(context, 'functionName');
  const dynamoEventRecords = event.Records;
  let consolNo;
  let orderId;
  let houseBillString = '';
  let stationCode = 'SUPPORT';
  let userEmail;
  try {
    const promises = dynamoEventRecords.map(async (record) => {
      let payload = '';

      try {
        const newImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
        consolNo = _.get(newImage, 'ConsolNo', 0);
        userEmail = await getUserEmail({ userId: _.get(newImage, 'UpdatedBy') });
        console.info('🚀 ~ file: index.js:40 ~ promises ~ userEmail:', userEmail);
        const {
          shipmentApar,
          shipmentHeader,
          shipmentDesc,
          consolStopHeaders,
          customer,
          references,
          users,
        } = await fetchDataFromTablesList(consolNo);
        orderId = _.map(shipmentApar, 'FK_OrderNo'); // comma separated orderNo's
        // Get an array of Housebills
        const houseBills = _.map(shipmentHeader, 'Housebill');
        houseBillString = _.join(houseBills);
        stationCode = '';
        for (const apar of shipmentApar) {
          const consolStationId = _.get(apar, 'FK_ConsolStationId', '');
          if (consolStationId !== '') {
            stationCode = consolStationId;
            break; // Stop iterating once a non-empty value is found
          }
        }
        console.info('🚀 ~ file: index.js:53 ~ promises ~ stationCode:', stationCode);
        if (stationCode === '') {
          throw new Error('No station code found in shipmentApar');
        }
        const mtPayloadData = await mtPayload(
          shipmentApar,
          shipmentHeader,
          shipmentDesc,
          consolStopHeaders,
          customer,
          references,
          users
        );
        console.info('🙂 -> file: index.js:114 -> mtPayloadData:', JSON.stringify(mtPayloadData));
        payload = mtPayloadData;
        // Check if payload is not null
        if (payload) {
          const equipmentType = _.get(payload, 'equipment_type_id', 'NA');
          console.info('🚀 ~ file: index.js:182 ~ promises ~ equipmentType:', equipmentType);
          if (equipmentType === 'NA') {
            throw new Error(
              `\nPlease populate the equipment type to tender this load.\nPayload: ${payload}`
            );
          }
          const result = await fetch204TableDataForConsole({
            consolNo,
          });
          console.info('🙂 -> file: index.js:114 -> result:', result);

          // Check if result has items and status is sent
          if (result.Items.length > 0) {
            const oldPayload = _.get(result, 'Items[0].Payload', null);

            // Compare oldPayload with payload constructed now
            if (_.isEqual(oldPayload, payload)) {
              console.info('Payload is the same as the old payload. Skipping further processing.');
              return 'Payload is the same as the old payload. Skipping further processing.';
            }
          }
        }

        const createPayloadResponse = await sendPayload({
          payload,
          consolNo,
          orderId: _.join(orderId),
          houseBillString,
        });
        console.info('🙂 -> file: index.js:149 -> createPayloadResponse:', createPayloadResponse);
        const shipmentId = _.get(createPayloadResponse, 'id', 0);

        // Call liveSendUpdate for each Housebill
        await Promise.all(
          houseBills.map(async (houseBill) => {
            await liveSendUpdate(houseBill, shipmentId);
          })
        );
        const movements = _.get(createPayloadResponse, 'movements', []);
        // Add "brokerage_status" to each movement
        const updatedMovements = movements.map((movement) => ({
          ...movement,
          brokerage_status: 'NEWOMNI',
          brokerage: true,
        }));
        // Update the movements array in the response
        _.set(createPayloadResponse, 'movements', updatedMovements);
        const stopsOfOriginalPayload = _.get(payload, 'stops', []);

        const contactNames = {};
        stopsOfOriginalPayload.forEach((stop) => {
          if (stop.order_sequence) {
            contactNames[stop.order_sequence] = stop.contact_name;
          }
        });
        const stopsOfUpdatedPayload = _.get(createPayloadResponse, 'stops', []);

        // Update the stops array in the response with contact names
        const updatedStops = stopsOfUpdatedPayload.map((stop) => ({
          ...stop,
          contact_name: contactNames[stop.order_sequence] || 'NA',
        }));

        // Update the stops array in the payload with the updated contact names
        _.set(createPayloadResponse, 'stops', updatedStops);
        const updatedOrderResponse = await updateOrders({
          payload: createPayloadResponse,
          consolNo,
          orderId: _.join(orderId),
          houseBillString,
        });

        await updateStatusTable({
          response: updatedOrderResponse,
          payload,
          ConsolNo: String(consolNo),
          status: STATUSES.SENT,
          Housebill: String(houseBillString),
          ShipmentId: String(shipmentId),
        });
        await insertInOutputTable({
          response: updatedOrderResponse,
          payload,
          ConsolNo: String(consolNo),
          status: STATUSES.SENT,
          Housebill: String(houseBillString),
          ShipmentId: String(shipmentId),
        });
        await Promise.all(
          orderId.map(async (orderNo) => {
            await updateOrderStatusTable({
              orderNo,
              response: updatedOrderResponse,
              payload,
              status: STATUSES.SENT,
              Housebill: String(houseBillString),
              ShipmentId: String(shipmentId),
            });
          })
        );
      } catch (error) {
        console.info('Error', error);
        await insertInOutputTable({
          ConsolNo: String(consolNo),
          response: error.message,
          payload,
          status: STATUSES.FAILED,
          Housebill: String(houseBillString),
        });
        await updateStatusTable({
          response: error.message,
          payload,
          ConsolNo: String(consolNo),
          status: STATUSES.FAILED,
          Housebill: String(houseBillString),
        });
        await Promise.all(
          orderId.map(async (orderNo) => {
            await updateOrderStatusTable({
              orderNo,
              response: error.message,
              payload,
              status: STATUSES.FAILED,
              Housebill: String(houseBillString),
            });
          })
        );
        throw error;
      }
      return false;
    });
    await Promise.all(promises);
    return true; // Modify if needed
  } catch (error) {
    console.error('Error', error);
    await sendSESEmail({
      functionName,
      message: ` ${error.message}
      \n Please check details on ${CONSOL_STATUS_TABLE}. Look for status FAILED.
      \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0.`,
      userEmail,
      subject: {
        Data: `PB ERROR NOTIFICATION - ${STAGE} ~ Housebill: ${houseBillString} / ConsolNo: ${consolNo}`,
        Charset: 'UTF-8',
      },
    });
    await publishSNSTopic({
      message: ` ${error.message}
      \n Please check details on ${CONSOL_STATUS_TABLE}. Look for status FAILED.
      \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0.`,
      stationCode,
      houseBillString,
      consolNo,
    });
    return false;
  }
};

async function updateStatusTable({
  ConsolNo,
  status,
  response,
  payload,
  Housebill,
  ShipmentId = 0,
}) {
  try {
    const updateParam = {
      TableName: CONSOL_STATUS_TABLE,
      Key: { ConsolNo },
      UpdateExpression:
        'set #Status = :status, #Response = :response, Payload = :payload, Housebill = :housebill, ShipmentId = :shipmentid',
      ExpressionAttributeNames: {
        '#Status': 'Status',
        '#Response': 'Response',
      },
      ExpressionAttributeValues: {
        ':status': status,
        ':response': response,
        ':payload': payload,
        ':housebill': Housebill,
        ':shipmentid': ShipmentId,
      },
    };
    console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
    return await dynamoDb.update(updateParam).promise();
  } catch (err) {
    console.info('🙂 -> file: index.js:224 -> err:', err);
    throw err;
  }
}

async function updateOrderStatusTable({
  orderNo,
  status,
  response,
  payload,
  Housebill,
  ShipmentId = 0,
}) {
  try {
    const updateParam = {
      TableName: STATUS_TABLE,
      Key: { FK_OrderNo: orderNo },
      UpdateExpression:
        'set #Status = :status, #Response = :response, Payload = :payload, Housebill = :housebill, ShipmentId = :shipmentid',
      ExpressionAttributeNames: {
        '#Status': 'Status',
        '#Response': 'Response',
      },
      ExpressionAttributeValues: {
        ':status': status,
        ':response': response,
        ':payload': payload,
        ':housebill': Housebill,
        ':shipmentid': ShipmentId,
      },
    };
    console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
    return await dynamoDb.update(updateParam).promise();
  } catch (err) {
    console.info('🙂 -> file: index.js:224 -> err:', err);
    throw err;
  }
}

async function insertInOutputTable({
  ConsolNo,
  status,
  response,
  payload,
  Housebill,
  ShipmentId = 0,
}) {
  try {
    const params = {
      TableName: OUTPUT_TABLE,
      Item: {
        UUid: uuidv4(),
        ConsolNo,
        Status: status,
        Type: type,
        CreatedAt: moment.tz('America/Chicago').format(),
        Response: response,
        Payload: payload,
        Housebill,
        LastUpdateBy: functionName,
        ShipmentId,
      },
    };
    console.info('🙂 -> file: index.js:106 -> params:', params);
    await dynamoDb.put(params).promise();
    console.info('Record created successfully in output table.');
  } catch (err) {
    console.info('🙂 -> file: index.js:224 -> err:', err);
    throw err;
  }
}

async function publishSNSTopic({ message, stationCode, houseBillString, consolNo }) {
  try {
    const params = {
      TopicArn: LIVE_SNS_TOPIC_ARN,
      Subject: `PB ERROR NOTIFICATION - ${STAGE} ~ Housebill: ${houseBillString} / ConsolNo: ${consolNo}`,
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

async function fetch204TableDataForConsole({ consolNo }) {
  const params = {
    TableName: CONSOL_STATUS_TABLE,
    KeyConditionExpression: 'ConsolNo = :ConsolNo',
    ExpressionAttributeValues: {
      ':ConsolNo': consolNo,
    },
  };
  return await dynamoDb.query(params).promise();
}
