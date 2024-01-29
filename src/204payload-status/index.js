'use strict';
const AWS = require('aws-sdk');
const { prepareBatchFailureObj } = require('../shared/dataHelper');
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

module.exports.handler = async (event) => {
  console.info(
    '🙂 -> file: index.js:8 -> module.exports.handler= -> event:',
    JSON.stringify(event)
  );
  const dynamoEventRecords = event.Records;

  try {
    const promises = dynamoEventRecords.map(async (record) => {
      try {
        const shipmentAparData = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);

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

        // Non-Console
        if (
          parseInt(_.get(shipmentAparData, 'ConsolNo', 0), 10) === 0 &&
          _.includes(['HS', 'TL'], _.get(shipmentAparData, 'FK_ServiceId'))
        ) {
          const {
            shipmentHeaderData: [shipmentHeaderData],
            referencesData,
            shipmentDescData: [shipmentDescData],
            customersData: [customersData],
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
          });
          console.info(
            '🙂 -> file: index.js:114 -> nonConsolPayloadData:',
            JSON.stringify(nonConsolPayloadData)
          );
          return nonConsolPayloadData;
        }

        // Console
        if (
          parseInt(_.get(shipmentAparData, 'ConsolNo', 0), 10) > 0 &&
          _.get(shipmentAparData, 'Consolidation') === 'Y' &&
          _.includes(['HS', 'TL'], _.get(shipmentAparData, 'FK_ServiceId'))
        ) {
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
            shipmentDescData: [shipmentDescData],
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

          const nonConsolPayloadData = await consolPayload({
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
            '🙂 -> file: index.js:114 -> nonConsolPayloadData:',
            JSON.stringify(nonConsolPayloadData)
          );
          return 'Console payload';
        }
        if (
          parseInt(_.get(shipmentAparData, 'ConsolNo', null), 10) !== null &&
          _.get(shipmentAparData, 'Consolidation') === 'N' &&
          _.includes(['MT'], _.get(shipmentAparData, 'FK_ServiceId'))
        ) {
          const { shipmentHeader, shipmentDesc, consolStopHeaders, customer, references,users } =
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
          return mtPayloadData;
        }
      } catch (error) {
        console.info('Error', error);
      }
      return false;
    });

    await Promise.all(promises);

    return prepareBatchFailureObj([]); // Modify if needed
  } catch (error) {
    console.error('Error', error);
    return prepareBatchFailureObj(dynamoEventRecords); // Modify if needed
  }
};
