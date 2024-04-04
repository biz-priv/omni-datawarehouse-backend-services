'use strict';
const AWS = require('aws-sdk');
const _ = require('lodash');
const moment = require('moment-timezone');
const { v4: uuidv4 } = require('uuid');
const { nonConsolPayload, consolPayload } = require('../shared/204-payload-generator/payloads');
const {
  fetchLocationId,
  getFinalShipperAndConsigneeData,
  fetchCommonTableData,
  fetchNonConsoleTableData,
  fetchConsoleTableData,
  getShipmentHeaderData,
  getAparDataByConsole,
  getUserEmail,
  sendSESEmail,
} = require('../shared/204-payload-generator/helper');
const {
  sendPayload,
  updateOrders,
  liveSendUpdate,
} = require('../shared/204-payload-generator/apis');
const { STATUSES, TYPES } = require('../shared/constants/204_create_shipment');

const { STATUS_TABLE, LIVE_SNS_TOPIC_ARN, OUTPUT_TABLE, STAGE } = process.env;
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

let functionName;
let type;

module.exports.handler = async (event, context) => {
  console.info(
    '🙂 -> file: index.js:8 -> module.exports.handler= -> event:',
    JSON.stringify(event)
  );
  functionName = _.get(context, 'functionName');
  const dynamoEventRecords = event.Records;
  let stationCode = 'SUPPORT';
  let houseBillString = '';
  let orderId;
  let houseBill;
  let consolNo = 0;
  let userEmail;
  try {
    const promises = dynamoEventRecords.map(async (record) => {
      let payload = '';

      try {
        const newImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
        const shipmentAparData = _.get(newImage, 'ShipmentAparData');
        orderId = _.get(shipmentAparData, 'FK_OrderNo', 0);
        console.info('🙂 -> file: index.js:30 -> shipmentAparData:', shipmentAparData);
        type = _.get(newImage, 'Type', '');
        if (type === TYPES.MULTI_STOP) return 'Skipping for Multi Stops';

        // Non-Console
        if (
          parseInt(_.get(shipmentAparData, 'ConsolNo', 0), 10) === 0 &&
          _.includes(['HS', 'TL'], _.get(shipmentAparData, 'FK_ServiceId'))
        ) {
          userEmail = await getUserEmail({ userId: _.get(shipmentAparData, 'UpdatedBy') });
          console.info('🚀 ~ file: index.js:65 ~ promises ~ userEmail:', userEmail);
          const {
            shipmentHeaderData: [shipmentHeaderData],
            referencesData,
            shipmentDescData,
            customersData: [customersData],
            userData: [userData],
          } = await fetchNonConsoleTableData({ shipmentAparData });
          if (!shipmentHeaderData) {
            return 'Shipment header data is missing.';
          }
          houseBill = _.get(shipmentHeaderData, 'Housebill', 0);
          console.info('🚀 ~ file: index.js:71 ~ promises ~ houseBill:', houseBill);
          houseBillString = houseBill.toString();
          stationCode =
            _.get(shipmentHeaderData, 'ControllingStation', '') ||
            _.get(shipmentAparData, 'FK_HandlingStation', '');

          if (!stationCode) {
            throw new Error('Please populate the Controlling Station');
          }
          const {
            shipperLocationId,
            consigneeLocationId,
            finalConsigneeData,
            finalShipperData,
            confirmationCostData,
          } = await consolNonConsolCommonData({
            shipmentAparData,
            orderId,
            consolNo,
            houseBill,
          });
          const nonConsolPayloadData = await nonConsolPayload({
            referencesData,
            customersData,
            consigneeLocationId,
            finalConsigneeData,
            finalShipperData,
            shipmentDesc: shipmentDescData,
            shipmentHeader: shipmentHeaderData,
            shipperLocationId,
            userData,
            shipmentAparData,
            confirmationCostData,
          });
          console.info(
            '🙂 -> file: index.js:114 -> nonConsolPayloadData:',
            JSON.stringify(nonConsolPayloadData)
          );
          payload = nonConsolPayloadData;
        }

        // Console
        if (
          parseInt(_.get(shipmentAparData, 'ConsolNo', 0), 10) > 0 &&
          _.get(shipmentAparData, 'Consolidation') === 'Y' &&
          _.includes(['HS', 'TL'], _.get(shipmentAparData, 'FK_ServiceId'))
        ) {
          consolNo = _.get(shipmentAparData, 'ConsolNo', 0);
          userEmail = await getUserEmail({ userId: _.get(shipmentAparData, 'UpdatedBy') });
          console.info('🚀 ~ file: index.js:120 ~ promises ~ userEmail:', userEmail);
          const {
            shipmentHeaderData: [shipmentHeaderData],
            customersData: [customersData],
            userData: [userData],
          } = await fetchConsoleTableData({ shipmentAparData });
          console.info('🙂 -> file: index.js:118 -> userData:', userData);
          console.info('🙂 -> file: index.js:119 -> customersData:', customersData);
          console.info('🙂 -> file: index.js:123 -> shipmentHeaderData:', shipmentHeaderData);
          const shipmentAparConsoleData = await getAparDataByConsole({
            shipmentAparData,
          });
          const shipmentHeader = await getShipmentHeaderData({
            shipmentAparConsoleData,
          });
          stationCode =
            _.get(shipmentHeaderData, 'ControllingStation', '') ||
            _.get(shipmentAparData, 'FK_ConsolStationId', '');

          if (!stationCode) {
            throw new Error('Please populate the Controlling Station');
          }
          houseBill = shipmentHeader.flatMap((headerData) => headerData.housebills);
          console.info('🚀 ~ file: index.js:123 ~ promises ~ houseBill:', houseBill);
          houseBillString = houseBill.join(',').toString();
          const {
            shipperLocationId,
            consigneeLocationId,
            finalConsigneeData,
            finalShipperData,
            confirmationCostData,
          } = await consolNonConsolCommonData({
            shipmentAparData,
            consolNo,
            orderId,
            houseBill: houseBill.join(','),
          });
          const ConsolPayloadData = await consolPayload({
            customersData,
            consigneeLocationId,
            finalConsigneeData,
            finalShipperData,
            shipmentHeader: shipmentHeaderData,
            shipperLocationId,
            shipmentAparData,
            userData,
            confirmationCostData,
          });
          console.info(
            '🙂 -> file: index.js:114 -> ConsolPayloadData:',
            JSON.stringify(ConsolPayloadData)
          );
          payload = ConsolPayloadData;
        }
        if (payload) {
          const equipmentType = _.get(payload, 'equipment_type_id', 'NA');
          console.info('🚀 ~ file: index.js:182 ~ promises ~ equipmentType:', equipmentType);
          if (equipmentType === 'NA') {
            throw new Error(
              `\n\nError Description:
               \nThe system encountered an error while processing the payload. It appears that the Equipment Type is not populated, which is necessary to tender this load.
               \nResolution:
               \nKindly fill in the Equipment Type and retry to successfully tender this load.
               \nPayload:
               \n\n${JSON.stringify(payload)}`
            );
          }
          const result = await fetch204TableDataForConsole({
            orderNo: orderId,
          });
          console.info('🙂 -> file: index.js:114 -> result:', result);

          // Check if result has items and status is sent
          if (result.Items.length > 0) {
            const oldPayload = _.get(result, 'Items[0].Payload', null);

            // Compare oldPayload with payload constructed now
            if (_.isEqual(oldPayload, payload)) {
              console.info('Payload is the same as the old payload. Skipping further processing.');
              throw new Error(`Payload is the same as the old payload. Skipping further processing.
              \n Payload: ${JSON.stringify(payload)}
              `);
            }
          }
        }
        const createPayloadResponse = await sendPayload({
          payload,
          orderId,
          consolNo,
          houseBillString,
        });
        console.info('🙂 -> file: index.js:149 -> createPayloadResponse:', createPayloadResponse);
        const shipmentId = _.get(createPayloadResponse, 'id', 0);
        if (type === 'NON_CONSOLE') {
          await liveSendUpdate(houseBill, shipmentId);
        } else if (type === 'CONSOLE') {
          // Mapping houseBills to promises
          const houseBillPromises = houseBill.map(async (hb) => {
            await liveSendUpdate(hb, shipmentId);
          });
          await Promise.all(houseBillPromises);
        }
        const movements = _.get(createPayloadResponse, 'movements', []);
        // Add "brokerage_status" to each movement
        const updatedMovements = movements.map((movement) => ({
          ...movement,
          brokerage_status: 'NEWOMNI',
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
          orderId,
          consolNo,
          houseBillString,
        });

        await updateStatusTable({
          response: updatedOrderResponse,
          payload,
          orderNo: orderId,
          status: STATUSES.SENT,
          Housebill: String(houseBillString),
          ShipmentId: String(shipmentId),
        });
        await insertInOutputTable({
          response: updatedOrderResponse,
          payload,
          orderNo: orderId,
          status: STATUSES.SENT,
          Housebill: String(houseBillString),
          ShipmentId: String(shipmentId),
        });
      } catch (error) {
        console.info('Error', error);
        await updateStatusTable({
          orderNo: orderId,
          response: error.message,
          payload,
          status: STATUSES.FAILED,
          Housebill: String(houseBillString),
        });
        await insertInOutputTable({
          response: error.message,
          payload,
          orderNo: orderId,
          status: STATUSES.FAILED,
          Housebill: String(houseBillString),
        });
        throw error;
      }
      return false;
    });
    await Promise.all(promises);
    return true;
  } catch (error) {
    console.error('Error', error);
    await sendSESEmail({
      functionName,
      message: ` ${error.message}
      \n Please check details on ${STATUS_TABLE}. Look for status FAILED.
      \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0.`,
      userEmail,
      subject: {
        Data: `PB ERROR NOTIFICATION - ${STAGE} ~ Housebill: ${houseBillString} / ConsolNo: ${consolNo}`,
        Charset: 'UTF-8',
      },
    });
    await publishSNSTopic({
      message: ` ${error.message}
      \n Please check details on ${STATUS_TABLE}. Look for status FAILED.
      \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0.`,
      stationCode,
      houseBillString,
      consolNo,
    });
    return false;
  }
};

async function consolNonConsolCommonData({ shipmentAparData, orderId, consolNo, houseBill }) {
  try {
    const {
      confirmationCostData: [confirmationCostData],
      shipperData: [shipperData],
      consigneeData: [consigneeData],
    } = await fetchCommonTableData({
      shipmentAparData,
    });

    console.info('🙂 -> file: index.js:35 -> consigneeData:', consigneeData);
    console.info('🙂 -> file: index.js:36 -> shipperData:', shipperData);
    console.info('🙂 -> file: index.js:37 -> confirmationCostData:', confirmationCostData);

    const { finalConsigneeData, finalShipperData } = getFinalShipperAndConsigneeData({
      confirmationCostData,
      consigneeData,
      shipperData,
    });

    const { shipperLocationId, consigneeLocationId } = await fetchLocationId({
      finalConsigneeData,
      finalShipperData,
      orderId,
      consolNo,
      houseBill,
    });

    console.info(
      '🙂 -> file: index.js:83 -> promises ->  shipperLocationId, consigneeLocationId:',
      shipperLocationId,
      consigneeLocationId
    );

    if (!shipperLocationId || !consigneeLocationId) {
      console.error('Could not fetch location id.');
      throw new Error('Could not fetch location id.');
    }
    return {
      shipperLocationId,
      consigneeLocationId,
      finalConsigneeData,
      finalShipperData,
      confirmationCostData,
    };
  } catch (error) {
    console.error('Error', error);
    throw new Error(`Error in consolNonConsolCommonData: ${error}`);
  }
}

async function updateStatusTable({
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
    console.error('🙂 -> file: index.js:224 -> err:', err);
    throw err;
  }
}

async function insertInOutputTable({
  orderNo,
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
        FK_OrderNo: orderNo,
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
    console.error('🙂 -> file: index.js:296 -> err:', err);
    throw err;
  }
}

async function publishSNSTopic({ message, stationCode, houseBillString, consolNo = 0 }) {
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

async function fetch204TableDataForConsole({ orderNo }) {
  try {
    const params = {
      TableName: STATUS_TABLE,
      KeyConditionExpression: 'FK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
      ProjectionExpression: 'FK_OrderNo, Payload',
    };
    return await dynamoDb.query(params).promise();
  } catch (error) {
    console.info('🚀 ~ file: index.js:324 ~ fetch204TableDataForConsole ~ error:', error);
    throw error;
  }
}
