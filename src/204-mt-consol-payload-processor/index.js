'use strict';
const AWS = require('aws-sdk');
const _ = require('lodash');
const moment = require('moment-timezone');
const { v4: uuidv4 } = require('uuid');

const { mtPayload } = require('../shared/204-payload-generator/payloads');
const { fetchDataFromTablesList } = require('../shared/204-payload-generator/helper');
const { sendPayload, updateOrders } = require('../shared/204-payload-generator/apis');
const { STATUSES } = require('../shared/constants/204_create_shipment');

const { SNS_TOPIC_ARN, CONSOL_STATUS_TABLE, OUTPUT_TABLE } = process.env;
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

  try {
    const promises = dynamoEventRecords.map(async (record) => {
      let payload = '';
      let consolNo;

      try {
        const newImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
        const shipmentAparData = _.get(newImage, 'ShipmentAparData');
        consolNo = _.get(shipmentAparData, 'ConsolNo', 0);
        console.info('🙂 -> file: index.js:30 -> shipmentAparData:', shipmentAparData);
        type = _.get(newImage, 'Type', '');

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
            const result = await fetch204TableDataForConsole({
              consolNo,
            });
            console.info('🙂 -> file: index.js:114 -> result:', result);

            // Check if result has items and status is sent
            if (result.Items.length > 0) {
              const oldPayload = _.get(result, 'Items[0].Payload', null);

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
          ConsolNo: consolNo,
          status: STATUSES.SENT,
        });
        await insertInOutputTable({
          response: updatedOrderResponse,
          payload,
          ConsolNo: consolNo,
          status: STATUSES.SENT,
        });
      } catch (error) {
        console.info('Error', error);
        await insertInOutputTable({
          ConsolNo: consolNo,
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
      \n Please check details on ${CONSOL_STATUS_TABLE}. Look for status FAILED.
      \n Retrigger the process by changes Status to ${STATUSES.PENDING} and reset the RetryCount to 0.`,
    });
    return false;
  }
};

async function updateStatusTable({ ConsolNo, status, response, payload }) {
  try {
    const updateParam = {
      TableName: CONSOL_STATUS_TABLE,
      Key: { ConsolNo },
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

async function insertInOutputTable({ ConsolNo, status, response, payload }) {
  try {
    const params = {
      TableName: OUTPUT_TABLE,
      Item: {
        UUiD: uuidv4(),
        ConsolNo,
        Status: status,
        Type: type,
        CreatedAt: moment.tz('America/Chicago').format(),
        Response: response,
        Payload: payload,
        LastUpdateBy: functionName,
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
