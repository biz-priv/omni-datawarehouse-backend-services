const AWS = require("aws-sdk");
const s3 = new AWS.S3();
const moment = require("moment-timezone");

async function getS3Data(bucket, key) {
  let params;
  try {
    params = { Bucket: bucket, Key: key };
    const response = await s3.getObject(params).promise();
    return response.Body.toString();
  } catch (error) {
    console.error("Read s3 data: ", e, "\ns3 params: ", params);
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
    console.info("Insert Params: ", params);
    const dynamoInsert = await dynamodb.put(params).promise();
    return dynamoInsert;
  } catch (e) {
    console.error("Put Item Error: ", e, "\nPut params: ", params);
    throw error;
  }
}

async function prepareHeaderLevelAndReferenceListData(xmlObj, s3folderName) {
  try {
    const consigneeData = get(
      xmlObj1,
      "UniversalShipment.Shipment.OrganizationAddressCollection.OrganizationAddress",
      []
    ).filter((obj) => obj.AddressType === "ConsigneeAddress");
    const shipperData = get(
      xmlObj1,
      "UniversalShipment.Shipment.OrganizationAddressCollection.OrganizationAddress",
      []
    ).filter((obj) => obj.AddressType === "ConsignorPickupDeliveryAddress");

    const headerData = {
      DeclaredType: "LL",
      ServiceLevel: "EC",
      ShipmentType: "Shipment",
      Mode: "Domestic",
      Station: get(
        CONSTANTS,
        `Station.${get(xmlObj1, "UniversalShipment.Shipment.Order.Warehouse.Code", "")}`,
        ""
      ),
      ConsigneeAddress1: get(consigneeData, "[0].Address1", ""),
      ConsigneeAddress2: get(consigneeData, "[0].Address2", ""),
      ConsigneeCity: get(consigneeData, "[0].City", ""),
      ConsigneeName: get(consigneeData, "[0].CompanyName", ""),
      ConsigneeCountry: get(consigneeData, "[0].Country.Code", ""),
      ConsigneeEmail: get(consigneeData, "[0].Email", ""),
      ConsigneePhone: get(consigneeData, "[0].Phone", ""),
      ConsigneeZip: get(consigneeData, "[0].Postcode", ""),
      ConsigneeState: get(consigneeData, "[0].State._", ""),
      CustomerNo: get(CONSTANTS, `billToAcc.${s3folderName}`, ""),
      BillToAcct: get(CONSTANTS, `billToAcc.${s3folderName}`, ""),
      ReferenceList: {
        NewShipmentRefsV3: [
          {
            ReferenceNo: get(
              xmlObj1,
              "UniversalShipment.Shipment.DataContext.DataSourceCollection.DataSource.Key",
              ""
            ),
            CustomerTypeV3: "Shipper",
            RefTypeId: "ORD",
          },
          {
            ReferenceNo: get(
              xmlObj1,
              "UniversalShipment.Shipment.Order.OrderNumber",
              ""
            ),
            CustomerTypeV3: "BillTo",
            RefTypeId: "INV",
          },
        ],
      },
    };

    if (s3folderName == "RoyalEnfield") {
      headerData.ShipperAddress1 = "1010 S INDUSTRIAL BLVD BLDG B";
      headerData.ShipperAddress2 = "";
      headerData.ShipperCity = "EULESS";
      headerData.ShipperName = "OMNI LOGISTICS, LLC.";
      headerData.ShipperCountry = "US";
      headerData.ShipperEmail = "";
      headerData.ShipperPhone = "";
      headerData.ShipperZip = "76040";
      headerData.ShipperState = "TX";
    } else {
      headerData.ShipperAddress1 = get(shipperData, "[0].Address1", "");
      headerData.ShipperAddress2 = get(shipperData, "[0].Address2", "");
      headerData.ShipperCity = get(shipperData, "[0].City", "");
      headerData.ShipperName = get(shipperData, "[0].CompanyName", "");
      headerData.ShipperCountry = get(shipperData, "[0].Country.Code", "");
      headerData.ShipperEmail = get(shipperData, "[0].Email", "");
      headerData.ShipperPhone = get(shipperData, "[0].Phone", "");
      headerData.ShipperZip = get(shipperData, "[0].Postcode", "");
      headerData.ShipperState = get(shipperData, "[0].State._", "");
    }

    const dateDeliveryBy = get(
      xmlObj1,
      "UniversalShipment.Shipment.LocalProcessing.DeliveryRequiredBy",
      ""
    );
    const dateFlag = !isNaN(new Date(dateDeliveryBy).getTime());
    if (dateFlag) {
      headerData.ReadyDate = dateDeliveryBy;
      const dateValue = moment(dateDeliveryBy).startOf("day");
      headerData.ReadyTime = dateValue
        .add(8, "hours")
        .format("YYYY-MM-DDTHH:mm:ss");
      headerData.CloseTime = dateValue
        .add(17, "hours")
        .format("YYYY-MM-DDTHH:mm:ss");
    }
    return headerData;
  } catch (error) {
    console.error(
      "Error while preparing headerLevel and referenceList data: ",
      error
    );
    throw error;
  }
}

const CONSTANTS = {
  Station: {
    DF1: "DFW",
  },
  billToAcc: {
    RoyalEnfield: "53241",
  },
  shipperFlag: {
    RoyalEnfield: true,
  },
};

async function prepareShipmentListData(xmlObj1, s3folderName) {
  try {
    let ShipmentLineList = {};
    const orderLineArray = get(
      xmlObj1,
      "UniversalShipment.Shipment.Order.OrderLineCollection.OrderLine",
      {}
    );

    if (s3folderName == "RoyalEnfield") {
      if (Array.isArray(orderLineArray)) {
        ShipmentLineList.NewShipmentDimLineV3 = [];
        await Promise.all(
          orderLineArray.map(async (line) => {
            const data = {
              WeightUOMV3: "lb",
              Description: get(line, "PartAttribute1", ""),
              DimUOMV3: "in",
              PieceType: "UNT",
              Pieces: Number(get(line, "QuantityMet", 0)),
              Weigth: 600,
              Length: 89,
              Width: 48,
              Height: 31,
            };
            ShipmentLineList.NewShipmentDimLineV3.push(data);
          })
        );
      } else {
        ShipmentLineList = {
          NewShipmentDimLineV3: [
            {
              WeightUOMV3: "lb",
              Description: get(orderLineArray, "PartAttribute1", ""),
              DimUOMV3: "in",
              PieceType: "UNT",
              Pieces: Number(get(orderLineArray, "QuantityMet", 0)),
              Weigth: 600,
              Length: 89,
              Width: 48,
              Height: 31,
            },
          ],
        };
      }
    } else {
      if (Array.isArray(orderLineArray)) {
        ShipmentLineList.NewShipmentDimLineV3 = [];
        await Promise.all(
          orderLineArray.map(async (line) => {
            const data = {
              WeightUOMV3: get(line, "Weight", ""),
              Description: get(line, "PartAttribute1", ""),
              DimUOMV3: get(line, "Dim", ""),
              PieceType: get(line, "Peice", ""),
              Pieces: Number(get(line, "QuantityMet", 0)),
              Weigth: Number(get(line, "Weigth", "")),
              Length: Number(get(line, "Length", "")),
              Width: Number(get(line, "Width", "")),
              Height: Number(get(line, "Height", "")),
            };
            ShipmentLineList.NewShipmentDimLineV3.push(data);
          })
        );
      } else {
        ShipmentLineList = {
          NewShipmentDimLineV3: [
            {
              WeightUOMV3: get(orderLineArray, "Weight", ""),
              Description: get(orderLineArray, "PartAttribute1", ""),
              DimUOMV3: get(orderLineArray, "Dim", ""),
              PieceType: get(orderLineArray, "Peice", ""),
              Pieces: Number(get(orderLineArray, "QuantityMet", 0)),
              Weigth: Number(get(orderLineArray, "Weigth", "")),
              Length: Number(get(orderLineArray, "Length", "")),
              Width: Number(get(orderLineArray, "Width", "")),
              Height: Number(get(orderLineArray, "Height", "")),
            },
          ],
        };
      }
    }
  } catch (error) {
    console.error("Error while preparing shipment list: ", error);
    throw error;
  }
}

module.exports = {
  putItem,
  getS3Data,
  prepareShipmentListData,
  prepareHeaderLevelAndReferenceListData,
};
