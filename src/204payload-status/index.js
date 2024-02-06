'use strict';
const AWS = require('aws-sdk');
const _ = require('lodash');
const { nonConsolPayload, consolPayload, mtPayload } = require('./payloads');
const {
  fetchLocationId,
  getFinalShipperAndConsigneeData,
  fetchAparTableForConsole,
  fetchCommonTableData,
  fetchNonConsoleTableData,
  fetchConsoleTableData,
  fetchDataFromTablesList,
} = require('./helper');
const { sendPayload, updateOrders } = require('./apis');
const { STATUSES } = require('../shared/constants/204_create_shipment');

const { STATUS_TABLE, SNS_TOPIC_ARN, CONSOL_STATUS_TABLE, STATUS_TABLE_CONSOLE_INDEX } =
  process.env;
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

let functionName;

module.exports.handler = async (event, context) => {
  console.info(
    '🙂 -> file: index.js:8 -> module.exports.handler= -> event:',
    JSON.stringify(event)
  );
  functionName = _.get(context, 'functionName');
  const dynamoEventRecords = event.Records;

  try {
    const promises = dynamoEventRecords.map(async (record) => {
      let payload = '';
      let orderId;

      try {
        const newImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
        const shipmentAparData = _.get(newImage, 'ShipmentAparData');
        orderId = _.get(shipmentAparData, 'FK_OrderNo', 0);
        console.info('🙂 -> file: index.js:30 -> shipmentAparData:', shipmentAparData);
        const type = _.get(newImage, 'Type', '');

        // Non-Console
        if (
          parseInt(_.get(shipmentAparData, 'ConsolNo', 0), 10) === 0 &&
          _.includes(['HS', 'TL'], _.get(shipmentAparData, 'FK_ServiceId'))
        ) {
          const { shipperLocationId, consigneeLocationId, finalConsigneeData, finalShipperData } =
            await consolNonConsolCommonData({ shipmentAparData });
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
          const { shipperLocationId, consigneeLocationId, finalConsigneeData, finalShipperData } =
            await consolNonConsolCommonData({ shipmentAparData });
          const shipmentAparDataForConsole = await fetchAparTableForConsole({
            orderNo: _.get(shipmentAparData, 'FK_OrderNo'),
          });
          console.info(
            '🙂 -> file: index.js:121 -> shipmentAparDataForConsole:',
            shipmentAparDataForConsole
          );
          const {
            shipmentHeaderData: [shipmentHeaderData],
            referencesData,
            shipmentDescData,
            trackingNotesData: [trackingNotesData],
            customersData: [customersData],
            userData: [userData],
          } = await fetchConsoleTableData({ shipmentAparData });
          console.info('🙂 -> file: index.js:118 -> userData:', userData);
          console.info('🙂 -> file: index.js:119 -> customersData:', customersData);
          console.info('🙂 -> file: index.js:120 -> trackingNotesData:', trackingNotesData);
          console.info('🙂 -> file: index.js:121 -> shipmentDescData:', shipmentDescData);
          console.info('🙂 -> file: index.js:122 -> referencesData:', referencesData);
          console.info('🙂 -> file: index.js:123 -> shipmentHeaderData:', shipmentHeaderData);

          const ConsolPayloadData = await consolPayload({
            referencesData,
            customersData,
            consigneeLocationId,
            finalConsigneeData,
            finalShipperData,
            shipmentDesc: shipmentDescData,
            shipmentHeader: shipmentHeaderData,
            shipperLocationId,
            shipmentAparData,
            trackingNotesData,
            userData,
          });
          console.info(
            '🙂 -> file: index.js:114 -> ConsolPayloadData:',
            JSON.stringify(ConsolPayloadData)
          );
          payload = ConsolPayloadData;
        }

        // MT Payload
        if (
          parseInt(_.get(shipmentAparData, 'ConsolNo', null), 10) !== null &&
          _.get(shipmentAparData, 'Consolidation') === 'N' &&
          _.includes(['MT'], _.get(shipmentAparData, 'FK_ServiceId'))
        ) {
          const { shipmentHeader, shipmentDesc, consolStopHeaders, customer, references, users } =
            await fetchDataFromTablesList(_.get(shipmentAparData, 'ConsolNo', null));

          const mtPayloadData = await mtPayload(
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
            const result = await fetch204TableDataForConsole(
              _.get(shipmentAparData, 'ConsolNo', null)
            );
            console.info('🙂 -> file: index.js:114 -> result:', result);

            // Check if result has items and status is sent
            if (result.Items.length > 0) {
              const oldPayload = _.get(result, 'Items[0].payload', null);

              // Compare oldPayload with payload constructed now
              if (_.isEqual(oldPayload, payload)) {
                console.info(
                  'Payload is the same as the old payload. Skipping further processing.'
                );
                return 'Payload is the same as the old payload. Skipping further processing.';
              }
            }
          }
        }

        const createPayloadResponse = await sendPayload({ payload });
        console.info('🙂 -> file: index.js:149 -> createPayloadResponse:', createPayloadResponse);

        const movements = _.get(createPayloadResponse, 'movements', []);
        // Add "brokerage_status" to each movement
        const updatedMovements = movements.map((movement) => ({
          ...movement,
          brokerage_status: 'NEWOMNI',
        }));
        // Update the movements array in the response
        _.set(createPayloadResponse, 'movements', updatedMovements);

        const updatedOrderResponse = await updateOrders({ payload: createPayloadResponse });

        await updateStatusTable({
          response: updatedOrderResponse,
          payload,
          orderNo: orderId,
          status: STATUSES.SENT,
        });
        if (type === 'MULTI_STOP') {
          await updateConsoleTable({ consolNo: _.get(shipmentAparData, 'ConsolNo', null) });
        }
      } catch (error) {
        console.info('Error', error);
        await updateStatusTable({
          orderNo: orderId,
          response: error.message,
          payload,
          status: STATUSES.FAILED,
        });
        throw error;
      }
      return false;
    });
    await Promise.all(promises);
    return true; // Modify if needed
  } catch (error) {
    console.error('Error', error);
    await publishSNSTopic({
      message: ` ${error.message}
      \n Please check details on ${STATUS_TABLE}. Look for status FAILED.
      \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0.`,
    });
    return false;
  }
};

async function consolNonConsolCommonData({ shipmentAparData }) {
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

    if (!consigneeData || !shipperData) {
      console.error('Shipper or Consignee tables are not populated.');
      throw new Error('Shipper or Consignee tables are not populated.');
    }

    const { finalConsigneeData, finalShipperData } = getFinalShipperAndConsigneeData({
      confirmationCostData,
      consigneeData,
      shipperData,
    });

    const { shipperLocationId, consigneeLocationId } = await fetchLocationId({
      finalConsigneeData,
      finalShipperData,
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
    };
  } catch (error) {
    console.error('Error', error);
    throw new Error(`Error in consolNonConsolCommonData: ${error}`);
  }
}

async function updateStatusTable({ orderNo, status, response, payload }) {
  try {
    const updateParam = {
      TableName: STATUS_TABLE,
      Key: { FK_OrderNo: orderNo },
      UpdateExpression: 'set #Status = :status, #Response = :response, Payload = :payload',
      ExpressionAttributeNames: { '#Status': 'Status', '#Response': 'Response' },
      ExpressionAttributeValues: {
        ':status': status,
        ':response': response,
        ':payload': payload,
      },
    };
    console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
    return await dynamoDb.update(updateParam).promise();
  } catch (err) {
    console.info('🙂 -> file: index.js:224 -> err:', err);
    throw err;
  }
}

async function publishSNSTopic({ message }) {
  // return;
  await sns
    .publish({
      TopicArn: SNS_TOPIC_ARN,
      Subject: `Error on ${functionName} lambda.`,
      Message: `An error occurred: ${message}`,
    })
    .promise();
}

async function updateConsoleTable({ consolNo }) {
  try {
    const updateParam = {
      TableName: CONSOL_STATUS_TABLE,
      Key: { ConsolNo: consolNo },
      UpdateExpression: 'set #Status = :status,',
      ExpressionAttributeNames: { '#Status': 'Status' },
      ExpressionAttributeValues: {
        ':status': STATUSES.SENT,
      },
    };
    console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
    return await dynamoDb.update(updateParam).promise();
  } catch (err) {
    console.info('🙂 -> file: index.js:224 -> err:', err);
    throw err;
  }
}

async function fetch204TableDataForConsole({ consolNo }) {
  const params = {
    TableName: STATUS_TABLE,
    IndexName: STATUS_TABLE_CONSOLE_INDEX,
    KeyConditionExpression: 'ConsolNo = :ConsolNo',
    ExpressionAttributeValues: {
      ':ConsolNo': consolNo.toString(),
    },
  };
  return await dynamoDb.query(params).promise();
}
