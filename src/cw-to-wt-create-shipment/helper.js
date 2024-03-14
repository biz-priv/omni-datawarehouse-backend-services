'use strict';

const AWS = require('aws-sdk');
const moment = require('moment-timezone');
const { get } = require('lodash');

const s3 = new AWS.S3();
const dynamoDb = new AWS.DynamoDB.DocumentClient();

let orgCode = '';

async function getS3Data(bucket, key) {
  let params;
  try {
    params = { Bucket: bucket, Key: key };
    const response = await s3.getObject(params).promise();
    return response.Body.toString();
  } catch (error) {
    console.error('Read s3 data: ', error, '\ns3 params: ', params);
    throw error;
  }
}

async function putItem(item) {
  let params;
  try {
    params = {
      TableName: process.env.LOGS_TABLE,
      Item: item,
    };
    console.info('Insert Params: ', params);
    const dynamoInsert = await dynamoDb.put(params).promise();
    return dynamoInsert;
  } catch (error) {
    console.error('Put Item Error: ', error, '\nPut params: ', params);
    throw error;
  }
}

async function prepareHeaderLevelAndReferenceListData(xmlObj, statusCode) {
  try {
    const consigneeData = get(
      xmlObj,
      'UniversalShipment.Shipment.OrganizationAddressCollection.OrganizationAddress',
      []
    ).filter((obj) => obj.AddressType === 'ConsigneeAddress');
    const shipperData = get(
      xmlObj,
      'UniversalShipment.Shipment.OrganizationAddressCollection.OrganizationAddress',
      []
    ).filter((obj) => obj.AddressType === 'ConsignorPickupDeliveryAddress');
    const orgCodeData = get(
      xmlObj,
      'UniversalShipment.Shipment.OrganizationAddressCollection.OrganizationAddress',
      []
    ).filter((obj) => obj.AddressType === 'SendersLocalClient');
    orgCode = get(orgCodeData, '[0].OrganizationCode', '');
    if (orgCode === '') {
      throw new Error('Organization code is missing in the request data');
    }

    const headerData = {
      ServiceLevel: 'EC',
      PayType: 3,
      ShipmentType: 'Shipment',
      Mode: 'Domestic',
      Station: await getStationId(),
      ConsigneeAddress1: get(consigneeData, '[0].Address1', ''),
      ConsigneeAddress2: get(consigneeData, '[0].Address2', ''),
      ConsigneeCity: get(consigneeData, '[0].City', ''),
      ConsigneeName: get(consigneeData, '[0].CompanyName', ''),
      ConsigneeCountry: get(consigneeData, '[0].Country.Code', ''),
      ConsigneeEmail: get(consigneeData, '[0].Email', ''),
      ConsigneePhone: get(consigneeData, '[0].Phone', ''),
      ConsigneeZip: get(consigneeData, '[0].Postcode', ''),
      ConsigneeState: get(consigneeData, '[0].State._', ''),
      CustomerNo: get(CONSTANTS, `billToAcc.${orgCode}`, ''),
      BillToAcct: get(CONSTANTS, `billToAcc.${orgCode}`, ''),
    };

    if (orgCode === 'ROYENFMKE' || orgCode === 'DUCATI') {
      // If the status code is not 'DEP', we can ignore the event.
      if (statusCode !== 'ATP') {
        console.info('Status Code: ', statusCode);
        throw new Error(
          `Status Code is invalid, Status Code we recieved: ${statusCode}\nPlease correct the status code and send the request again.`
        );
      }

      if (orgCode === 'ROYENFMKE') {
        headerData.DeclaredType = 'LL';
        headerData.ShipperName = 'ROYAL ENFIELD NA LTD-EULESS';
      } else {
        const orderLineArray = get(
          xmlObj,
          'UniversalShipment.Shipment.Order.OrderLineCollection.OrderLine',
          {}
        );
        let PeiceCount = 0;
        if (Array.isArray(orderLineArray)) {
          for (const i of orderLineArray) {
            PeiceCount += Number(get(i, 'QuantityMet', 0));
          }
        } else {
          PeiceCount = Number(get(orderLineArray, 'QuantityMet', 0));
        }
        headerData.DeclaredType = 'INSP';
        headerData.DeclaredValue = 15000 * PeiceCount;
        headerData.ShipperName = 'DUCATI';
      }
      headerData.ShipperAddress1 = '1010 S INDUSTRIAL BLVD BLDG B';
      headerData.ShipperAddress2 = '';
      headerData.ShipperCity = 'EULESS';
      headerData.ShipperCountry = 'US';
      headerData.ShipperEmail = '';
      headerData.ShipperPhone = '';
      headerData.ShipperZip = '76040';
      headerData.ShipperState = 'TX';
      headerData.ReferenceList = {
        NewShipmentRefsV3: [
          {
            ReferenceNo: get(xmlObj, 'UniversalShipment.Shipment.Order.OrderNumber', ''),
            CustomerTypeV3: 'BillTo',
            RefTypeId: 'INV',
          },
          {
            ReferenceNo: get(xmlObj, 'UniversalShipment.Shipment.Order.OrderNumber', ''),
            CustomerTypeV3: 'Shipper',
            RefTypeId: 'INV',
          },
        ],
      };
    } else {
      if (statusCode !== 'DEP') {
        console.info('Status Code: ', statusCode);
        throw new Error(
          `Status Code is invalid, Status Code we recieved: ${statusCode}\nPlease correct the status code and send the request again.`
        );
      }
      headerData.DeclaredType = 'LL';
      headerData.ShipperAddress1 = get(shipperData, '[0].Address1', '');
      headerData.ShipperAddress2 = get(shipperData, '[0].Address2', '');
      headerData.ShipperCity = get(shipperData, '[0].City', '');
      headerData.ShipperName = get(shipperData, '[0].CompanyName', '');
      headerData.ShipperCountry = get(shipperData, '[0].Country.Code', '');
      headerData.ShipperEmail = get(shipperData, '[0].Email', '');
      headerData.ShipperPhone = get(shipperData, '[0].Phone', '');
      headerData.ShipperZip = get(shipperData, '[0].Postcode', '');
      headerData.ShipperState = get(shipperData, '[0].State._', '');
      headerData.ReferenceList = {
        NewShipmentRefsV3: [
          {
            ReferenceNo: get(
              xmlObj,
              'UniversalShipment.Shipment.DataContext.DataSourceCollection.DataSource.Key',
              ''
            ),
            CustomerTypeV3: 'Shipper',
            RefTypeId: 'WOR',
          },
        ],
      };
    }

    const dateDeliveryBy = get(
      xmlObj,
      'UniversalShipment.Shipment.LocalProcessing.DeliveryRequiredBy',
      ''
    );
    const dateFlag = !isNaN(new Date(dateDeliveryBy).getTime());
    if (dateFlag) {
      headerData.ReadyDate = dateDeliveryBy;
      const dateValue = moment(dateDeliveryBy).startOf('day');
      headerData.ReadyTime = dateValue.add(8, 'hours').format('YYYY-MM-DDTHH:mm:ss');
      headerData.CloseTime = dateValue.add(9, 'hours').format('YYYY-MM-DDTHH:mm:ss');
    }
    return headerData;
  } catch (error) {
    console.error('Error while preparing headerLevel and referenceList data: ', error);
    throw error;
  }
}

const CONSTANTS = {
  billToAcc: {
    ROYENFMKE: '53241',
    FEDSIGORD: '9058',
    SCIATLELP: '9606',
    OMNICEORD: '9269',
    ARJOHUORD: '8899',
    CUMALLORD: '8988',
    RTCINDRLM: '9660',
    WATERBECP: '23131',
    CIRKLIPHX: '52664',
    ARXIUMORD: '8907',
    DUCATI: '52380',
  },
};

async function prepareShipmentListData(xmlObj) {
  try {
    let ShipmentLineList = {};
    const orderLineArray = get(
      xmlObj,
      'UniversalShipment.Shipment.Order.OrderLineCollection.OrderLine',
      {}
    );

    if (orgCode === 'ROYENFMKE' || orgCode === 'DUCATI') {
      // values for royal enfield.
      let lengthValue = 89;
      let widthValue = 48;
      let heightValue = 31;
      let pieceTypeValue = 'UNT';

      if (orgCode === 'DUCATI') {
        lengthValue = 90;
        widthValue = 30;
        heightValue = 50;
        pieceTypeValue = 'CRT';
      }
      if (Array.isArray(orderLineArray)) {
        ShipmentLineList.NewShipmentDimLineV3 = orderLineArray.map((line) => {
          return {
            WeightUOMV3: 'lb',
            Description: get(line, 'PartAttribute1', ''),
            DimUOMV3: 'in',
            PieceType: pieceTypeValue,
            Pieces: Number(get(line, 'QuantityMet', 0)),
            Weigth: 600,
            Length: lengthValue,
            Width: widthValue,
            Height: heightValue,
          };
        });
      } else {
        ShipmentLineList = {
          NewShipmentDimLineV3: [
            {
              WeightUOMV3: 'lb',
              Description: get(orderLineArray, 'PartAttribute1', ''),
              DimUOMV3: 'in',
              PieceType: pieceTypeValue,
              Pieces: Number(get(orderLineArray, 'QuantityMet', 0)),
              Weigth: 600,
              Length: lengthValue,
              Width: widthValue,
              Height: heightValue,
            },
          ],
        };
      }
    } else if (Array.isArray(orderLineArray)) {
      ShipmentLineList.NewShipmentDimLineV3 = orderLineArray.map((line) => {
        return {
          WeightUOMV3: 'lb',
          Description: get(line, 'Product.Description', ''),
          DimUOMV3: 'in',
          PieceType: 'UNT',
          Pieces: Number(get(line, 'QuantityMet', 0)),
          Weigth: 1,
          Length: 1,
          Width: 1,
          Height: 1,
        };
      });
    } else {
      ShipmentLineList = {
        NewShipmentDimLineV3: [
          {
            WeightUOMV3: 'lb',
            Description: get(orderLineArray, 'Product.Description', ''),
            DimUOMV3: 'in',
            PieceType: 'UNT',
            Pieces: Number(get(orderLineArray, 'QuantityMet', 0)),
            Weigth: 1,
            Length: 1,
            Width: 1,
            Height: 1,
          },
        ],
      };
    }

    return { ShipmentLineList };
  } catch (error) {
    console.error('Error while preparing shipment list: ', error);
    throw error;
  }
}

async function getStationId() {
  const billToAcc = get(CONSTANTS, `billToAcc.${orgCode}`, '');
  if (billToAcc === '') {
    throw new Error(
      `Organization Code which is provided is not valid, The organization code that we receieve: ${orgCode}.\n Please contact omni support to add a organization code.`
    );
  }
  const params = {
    TableName: process.env.CUSTOMERS_TABLE,
    KeyConditionExpression: 'PK_CustNo = :Value',
    ExpressionAttributeValues: {
      ':Value': billToAcc,
    },
  };

  try {
    const data = await dynamoDb.query(params).promise();
    console.info('Query succeeded:', data);
    return get(data, 'Items[0].FK_CtrlStationId', '');
  } catch (err) {
    console.info('getStationId:', err);
    throw err;
  }
}

module.exports = {
  putItem,
  getS3Data,
  prepareShipmentListData,
  prepareHeaderLevelAndReferenceListData,
};
