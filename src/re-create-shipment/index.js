const AWS = require("aws-sdk");
const sns = new AWS.SNS();
const { get } = require("lodash");
const uuid = require("uuid");
const xml2js = require("xml2js");
const {
  prepareHeaderLevelAndReferenceListData,
  prepareShipmentListData,
  getS3Data,
  putItem,
} = require("./helper");

let dynamoData = {};
let s3folderName = "";

const xmlObj1 = {
  UniversalShipment: {
    xmlns: "http://www.cargowise.com/Schemas/Universal/2011/11",
    version: "1.1",
    Shipment: {
      DataContext: {
        DataSourceCollection: {
          DataSource: {
            Type: "WarehouseOrder",
            Key: "W00229650",
          },
        },
        ActionPurpose: {
          Code: "CSA",
          Description: "SEND TO WORLDTRAK",
        },
        Company: {
          Code: "ELP",
          Country: {
            Code: "US",
            Name: "United States",
          },
          Name: "OMNI LOGISTICS, LLC",
        },
        DataProvider: "TRXTS2ELP",
        EnterpriseID: "TRX",
        EventBranch: {
          Code: "DFW",
          Name: "OMNI LOGISTICS - DFW",
        },
        EventDepartment: {
          Code: "WHS",
          Name: "WAREHOUSE SERVICES",
        },
        EventType: {
          Code: "",
        },
        EventUser: {
          Code: "AWH",
          Name: "Alaina White",
        },
        ServerID: "TS2",
        TriggerCount: "1",
        TriggerDate: "2024-02-13T12:47:43.97",
        TriggerDescription: "",
        TriggerType: "Manual",
        RecipientRoleCollection: {
          RecipientRole: {
            Code: "CLI",
            Description: "Client",
          },
        },
      },
      CarrierServiceLevel: {
        Code: "",
      },
      CommercialInfo: {
        CommercialInvoiceCollection: {
          CommercialInvoice: {
            CommercialInvoiceLineCollection: {
              CommercialInvoiceLine: {
                LineNo: "0",
                Description: "ROYAL ENFIELD UNIT",
                InvoiceQuantity: "1.000",
                InvoiceQuantityUnit: {
                  Code: "UNT",
                  Description: "Unit",
                },
                Link: "0",
                OrderLineLink: "0",
                PartNo: "RENA",
                Weight: "600.000000",
                WeightUnit: {
                  Code: "LB",
                  Description: "Pounds",
                },
                CustomizedFieldCollection: {
                  CustomizedField: [
                    {
                      DataType: "String",
                      Key: "VIN",
                      Value: "ME3HJN47XRK700784",
                    },
                    {
                      DataType: "String",
                      Key: "Product Code",
                      Value: "RENA",
                    },
                  ],
                },
              },
            },
          },
        },
      },
      ContainerMode: {
        Code: "LTL",
        Description: "Less Truck Load",
      },
      GoodsDescription: "",
      GoodsValue: "0.0000",
      GoodsValueCurrency: {
        Code: "",
      },
      IsAuthorizedToLeave: "false",
      OuterPacks: "1",
      OuterPacksPackageType: {
        Code: "PCE",
        Description: "Piece",
      },
      PortOfDestination: {
        Code: "CAHAM",
        Name: "Hamilton",
      },
      PortOfOrigin: {
        Code: "USDFW",
        Name: "Dallas-Fort Worth Int Apt",
      },
      ScreeningStatus: {
        Code: "NOT",
        Description: "Not Screened",
      },
      ServiceLevel: {
        Code: "",
      },
      ShipmentIncoTerm: {
        Code: "FOB",
        Description: "Free On Board",
      },
      ShipperCODAmount: "0.0000",
      ShipperCODPayMethod: {
        Code: "",
      },
      TotalNoOfPacks: "1",
      TotalNoOfPacksPackageType: {
        Code: "PCE",
        Description: "Piece",
      },
      TotalVolume: "79.222",
      TotalVolumeUnit: {
        Code: "CF",
        Description: "Cubic Feet",
      },
      TotalWeight: "600.000",
      TotalWeightUnit: {
        Code: "LB",
        Description: "Pounds",
      },
      TransportMode: {
        Code: "ROA",
        Description: "Road Freight",
      },
      WayBillNumber: "",
      WayBillType: {
        Code: "HWB",
        Description: "House Waybill",
      },
      LocalProcessing: {
        DeliveryRequiredBy: "2024-02-13T23:59:00",
      },
      Order: {
        OrderNumber: "90002466TEST",
        AddPalletWeightToOrder: "false",
        ClientReference: "TEST",
        DropMode: {
          Code: "PSL",
          Description: "Premise Supplies Lift",
        },
        ExcludeFromTotePicking: "false",
        FulfillmentRule: {
          Code: "NON",
          Description: "None",
        },
        LocalCartageInsuranceValue: "0.0000",
        OrderNumberSplit: "0",
        PalletsSent: "0",
        PickOption: {
          Code: "AUT",
          Description: "Auto Pick",
        },
        RequiresPacking: "false",
        RequiresQualityAudit: "false",
        Status: {
          Code: "DEP",
          Description: "Departed",
        },
        TotalLineVolume: "79.222",
        TotalLineWeight: "600.000",
        TotalNetWeightSent: "600.000",
        TotalUnits: "0.000",
        TransportReference: "",
        Type: {
          Code: "ORD",
          Description: "ORDER",
        },
        UnitsSent: "1.000",
        Warehouse: {
          Code: "DF1",
          Name: "OMNI LOGISTICS - DFW BLDG B",
        },
        OrderLineCollection: {
          Content: "Complete",
          OrderLine: {
            Commodity: {
              Code: "",
            },
            ExpiryDate: "",
            ExtendedLinePrice: "0.0000",
            LineComment: "",
            LineNumber: "1",
            Link: "0",
            OrderedQty: "1.000",
            OrderedQtyUnit: {
              Code: "UNT",
              Description: "Unit",
            },
            PackageQty: "1.00",
            PackageQtyUnit: {
              Code: "UNT",
              Description: "Unit",
            },
            PackingDate: "",
            PartAttribute1: "ME3HJN47XRK700784",
            PartAttribute2: "",
            PartAttribute3: "",
            Product: {
              Code: "RENA",
              Description: "ROYAL ENFIELD UNIT",
            },
            QuantityMet: "1.000",
            ReservedQuantity: "0",
            SerialNumber: "",
            ShortfallQuantity: "0",
            SubLineNumber: "0",
            UnitPriceAfterDiscount: "0.0000",
            UnitPriceCurrency: {
              Code: "",
            },
            UnitPriceDiscountAmount: "0.0000",
            UnitPriceDiscountPercent: "0.00",
            UnitPriceRecommended: "0.0000",
          },
        },
      },
      MessageNumberCollection: {
        MessageNumber: [
          {
            _: "0e8ed1ee-448d-4c0a-bf55-977c3ecc64b8",
            Type: "TrackingID",
          },
          {
            _: "00000000000002768517",
            Type: "InterchangeNumber",
          },
          {
            _: "00000000000002641669",
            Type: "MessageNumber",
          },
        ],
      },
      MilestoneCollection: {
        Milestone: [
          {
            Description: "Entered",
            EventCode: "WHE",
            Sequence: "1",
            ActualDate: "2024-02-13T11:35:09.26",
            ConditionReference: "",
            ConditionType: "",
            EstimatedDate: "",
          },
          {
            Description: "Sent to Pick",
            EventCode: "WHI",
            Sequence: "2",
            ActualDate: "2024-02-13T11:35:14.22",
            ConditionReference: "",
            ConditionType: "",
            EstimatedDate: "",
          },
          {
            Description: "Finalized",
            EventCode: "FIN",
            Sequence: "3",
            ActualDate: "2024-02-13T11:35:44.6",
            ConditionReference: "",
            ConditionType: "",
            EstimatedDate: "",
          },
          {
            Description: "Released",
            EventCode: "PUP",
            Sequence: "4",
            ActualDate: "",
            ConditionReference: "",
            ConditionType: "",
            EstimatedDate: "",
          },
        ],
      },
      OrganizationAddressCollection: {
        OrganizationAddress: [
          {
            AddressType: "ConsignorDocumentaryAddress",
            Address1: "226 N WATER ST",
            Address2: "",
            AddressOverride: "false",
            AddressShortCode: "226 N WATER ST",
            City: "MILWAUKEE",
            CompanyName: "ROYAL ENFIELD NA LTD",
            Country: {
              Code: "US",
              Name: "United States",
            },
            Email: "krishnanr@royalenfield.com",
            Fax: "",
            GovRegNum: "47-352049100",
            GovRegNumType: {
              Code: "EIN",
              Description: "Employer Identification Number",
            },
            OrganizationCode: "ROYENFMKE",
            Phone: "+14145021204",
            Port: {
              Code: "USMKE",
              Name: "Milwaukee",
            },
            Postcode: "53202",
            ScreeningStatus: {
              Code: "NOT",
              Description: "Not Screened",
            },
            State: {
              _: "WI",
              Description: "Wisconsin",
            },
            RegistrationNumberCollection: {
              RegistrationNumber: {
                Type: {
                  Code: "LSC",
                  Description: "Legacy System Code",
                },
                CountryOfIssue: {
                  Code: "US",
                  Name: "United States",
                },
                Value: "111100692",
              },
            },
          },
          {
            AddressType: "SendersLocalClient",
            Address1: "226 N WATER ST",
            Address2: "",
            AddressOverride: "false",
            AddressShortCode: "226 N WATER ST",
            City: "MILWAUKEE",
            CompanyName: "ROYAL ENFIELD NA LTD",
            Country: {
              Code: "US",
              Name: "United States",
            },
            Email: "krishnanr@royalenfield.com",
            Fax: "",
            GovRegNum: "47-352049100",
            GovRegNumType: {
              Code: "EIN",
              Description: "Employer Identification Number",
            },
            OrganizationCode: "ROYENFMKE",
            Phone: "+14145021204",
            Port: {
              Code: "USMKE",
              Name: "Milwaukee",
            },
            Postcode: "53202",
            ScreeningStatus: {
              Code: "NOT",
              Description: "Not Screened",
            },
            State: {
              _: "WI",
              Description: "Wisconsin",
            },
            RegistrationNumberCollection: {
              RegistrationNumber: {
                Type: {
                  Code: "LSC",
                  Description: "Legacy System Code",
                },
                CountryOfIssue: {
                  Code: "US",
                  Name: "United States",
                },
                Value: "111100692",
              },
            },
          },
          {
            AddressType: "ConsigneeAddress",
            Address1: "1399 EIGHTH LINE",
            Address2: "",
            AddressOverride: "false",
            AddressShortCode: "1399 EIGHTH LINE",
            City: "LAKEFIELD",
            CompanyName: "CLASSY CHASSIS & CYCLES",
            Contact: "Jon Burman",
            Country: {
              Code: "CA",
              Name: "Canada",
            },
            Email: "revdadjon@gmail.com",
            Fax: "",
            Mobile: "",
            OrganizationCode: "CLACHAYHM",
            Phone: "+17058011322",
            Port: {
              Code: "CAHAM",
              Name: "Hamilton",
            },
            Postcode: "K0L 2H0",
            ScreeningStatus: {
              Code: "NOT",
              Description: "Not Screened",
            },
            State: {
              _: "ON",
              Description: "Ontario",
            },
            RegistrationNumberCollection: {
              RegistrationNumber: {
                Type: {
                  Code: "LSC",
                  Description: "Legacy System Code",
                },
                CountryOfIssue: {
                  Code: "US",
                  Name: "United States",
                },
                Value: "RENA62",
              },
            },
          },
          {
            AddressType: "TransportCompanyDocumentaryAddress",
            AdditionalAddressInformation: "BUILDING 1 & 2",
            Address1: "1010 S INDUSTRIAL BLVD",
            Address2: "",
            AddressOverride: "false",
            AddressShortCode: "1010 S INDUSTRIAL BLVD",
            City: "EULESS",
            CompanyName: "OMNI LOGISTICS, LLC-DFW",
            Country: {
              Code: "US",
              Name: "United States",
            },
            Email: "",
            Fax: "",
            GovRegNum: "76-065372502",
            GovRegNumType: {
              Code: "EIN",
              Description: "Employer Identification Number",
            },
            OrganizationCode: "OMNLOGDFW1",
            Phone: "+18174109225",
            Port: {
              Code: "USHOU",
              Name: "Houston",
            },
            Postcode: "76040",
            ScreeningStatus: {
              Code: "CLR",
              Description: "Clear",
            },
            State: {
              _: "TX",
              Description: "Texas",
            },
          },
          {
            AddressType: "ConsignorPickupDeliveryAddress",
            Address1: "1010 S INDUSTRIAL BLVD",
            Address2: "",
            AddressOverride: "false",
            AddressShortCode: "BLDG B",
            City: "EULESS",
            CompanyName: "OMNI LOGISTICS - DFW",
            Country: {
              Code: "US",
              Name: "United States",
            },
            Email: "",
            Fax: "",
            GovRegNum: "76-065372502",
            GovRegNumType: {
              Code: "EIN",
              Description: "Employer Identification Number",
            },
            OrganizationCode: "ICDFW",
            Phone: "+18174109225",
            Port: {
              Code: "USDFW",
              Name: "Dallas-Fort Worth Int Apt",
            },
            Postcode: "76040",
            ScreeningStatus: {
              Code: "CLR",
              Description: "Clear",
            },
            State: {
              _: "TX",
              Description: "Texas",
            },
          },
          {
            AddressType: "CustomsWarehouseAddress",
            Address1: "1010 S INDUSTRIAL BLVD",
            Address2: "",
            AddressOverride: "false",
            AddressShortCode: "BLDG B",
            City: "EULESS",
            CompanyName: "OMNI LOGISTICS - DFW",
            Country: {
              Code: "US",
              Name: "United States",
            },
            Email: "",
            Fax: "",
            GovRegNum: "76-065372502",
            GovRegNumType: {
              Code: "EIN",
              Description: "Employer Identification Number",
            },
            OrganizationCode: "ICDFW",
            Phone: "+18174109225",
            Port: {
              Code: "USDFW",
              Name: "Dallas-Fort Worth Int Apt",
            },
            Postcode: "76040",
            ScreeningStatus: {
              Code: "CLR",
              Description: "Clear",
            },
            State: {
              _: "TX",
              Description: "Texas",
            },
          },
        ],
      },
    },
  },
};

module.exports.handler = async (event, context) => {
  console.info("event: ", event);
  try {
    const s3Bucket = get(event, "Records[0].s3.bucket.name", "");
    const s3Key = get(event, "Records[0].s3.object.key", "");
    dynamoData = { s3Bucket, id: uuid.v4() };
    s3folderName = s3Key.split("/")[1];
    console.info("Id :", get(dynamoData, "id", ""));

    const s3Data = await getS3Data(s3Bucket, s3Key);

    const xmlObj = await xmlToJson(s3Data);
    dynamoData.xmlObj = xmlObj

    const xmlWTPayload = await prepareWTpayload(xmlObj);
    dynamoData.xmlWTPayload = xmlWTPayload

    const xmlWTResponse = await sendToWT(xmlWTPayload);
    dynamoData.xmlWTResponse = xmlWTResponse

    const xmlWTObjResponse = await xmlToJson(xmlWTResponse);
    dynamoData.xmlWTObjResponse = xmlWTObjResponse

    const xmlCWPayload = await prepareCWpayload(xmlObj1, xmlWTObjResponse);
    dynamoData.xmlCWPayload = xmlCWPayload

    const xmlCWResponse = await sendToCW(xmlCWPayload);
    dynamoData.xmlCWResponse = xmlCWResponse
    dynamoData.status = "SUCCESS";

    await putItem(dynamoData);

    console.log("xml object: ", xmlObj1);
    return "Success";
  } catch (error) {
    console.error("Main lambda error: ", error);

    try {
      const params = {
        Message: `An error occurred in function ${context.functionName}. Error details: ${error}.\n id: ${get(dynamoData, "id", "")} \n Note: Use the id for better search in the logs.`,
        TopicArn: process.env.ERROR_SNS_ARN,
      };
      await sns.publish(params).promise();
    } catch (err) {
      console.error("Error while sending sns notification: ", err);
    }
    dynamoData.errorMsg = error;
    dynamoData.status = "FAILED";
    await putItem(dynamoData);
  }
};

async function sendToWT(postData) {
  try {
    const config = {
      url: process.env.WT_URL,
      method: "post",
      headers: {
        "Content-Type": "text/xml",
        soapAction: "http://tempuri.org/AddNewShipmentV3",
      },
      data: postData,
    };

    console.log("config: ", config);
    const res = await axios.request(config);
    if (get(res, "status", "") == 200) {
      return get(res, "data", "");
    } else {
      itemObj.xmlWTResponse = get(res, "data", "");
      throw new Error(`API Request Failed: ${res}`);
    }
  } catch (error) {
    console.error("e:addMilestoneApi", error);
    throw error;
  }
}

async function sendToCW(postData) {
  try {
    const config = {
      url: process.env.CW_URL,
      method: "post",
      headers: {
        Authorization: "Basic dHJ4dHMyOjY0ODg1Nw==",
      },
      data: postData,
    };

    console.log("config: ", config);
    const res = await axios.request(config);
    if (get(res, "status", "") == 200) {
      return get(res, "data", "");
    } else {
      itemObj.xmlWTResponse = get(res, "data", "");
      throw new Error(`API Request Failed: ${res}`);
    }
  } catch (error) {
    console.error("e:addMilestoneApi", error);
    throw error;
  }
}

async function prepareWTpayload(xmlObj1) {
  try {
    const headerAndReferenceListData =
      await prepareHeaderLevelAndReferenceListData(xmlObj1, s3folderName);
    const shipmentListData = await prepareShipmentListData(
      xmlObj1,
      s3folderName
    );
    console.info(headerAndReferenceListData);
    console.info(shipmentListData);

    const finalData = {
      oshipData: {
        ...headerAndReferenceListData,
        ...shipmentListData,
      },
    };

    const xmlBuilder = new xml2js.Builder({
      render: {
        pretty: true,
        indent: "    ",
        newline: "\n",
      },
    });

    const xmlPayload = xmlBuilder.buildObject({
      "soap:Envelope": {
        $: {
          "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
          "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
          "xmlns:soap": "http://schemas.xmlsoap.org/soap/envelope/",
        },
        "soap:Header": {
          AuthHeader: {
            $: {
              xmlns: "http://tempuri.org/",
            },
            UserName: process.env.wt_soap_username,
            Password: process.env.wt_soap_password,
          },
        },
        "soap:Body": {
          AddNewShipmentV3: {
            $: {
              xmlns: "http://tempuri.org/",
            },
            oShipData: finalData.oshipData, // Insert your JSON data here
          },
        },
      },
    });

    return xmlPayload;
  } catch (error) {
    console.error("Error While preparing payload: ", error);
    throw error;
  }
}

async function xmlToJson(xmlData) {
  try {
    const parser = new xml2js.Parser({
      explicitArray: false,
      mergeAttrs: true,
    });
    return await parser.parseStringPromise(xmlData);
  } catch (error) {
    console.error("Error in xmlToJson: ", error);
    throw error;
  }
}

async function prepareCWpayload(xmlObj1, xmlWTObjResponse) {
  dynamoData.Housebill = get(
    xmlWTObjResponse,
    "soap:Envelope.soap:Body.AddNewShipmentV3Response.AddNewShipmentV3Result.Housebill",
    ""
  );
  dynamoData.fileNumber = get(
    xmlWTObjResponse,
    "soap:Envelope.soap:Body.AddNewShipmentV3Response.AddNewShipmentV3Result.ShipQuoteNo",
    ""
  );
  try {
    const payload = {
      DataContext: {
        DataTargetCollection: {
          DataTarget: {
            Type: "WarehouseOrder",
            Key: get(
              xmlObj1,
              "UniversalShipment.Shipment.DataContext.DataSourceCollection.DataSource.Key",
              ""
            ),
          },
        },
      },
      Order: {
        OrderNumber: get(
          xmlObj1,
          "UniversalShipment.Shipment.Order.OrderNumber",
          ""
        ),
        TransportReference: get(
          xmlWTObjResponse,
          "soap:Envelope.soap:Body.AddNewShipmentV3Response.AddNewShipmentV3Result.Housebill",
          ""
        ),
      },
    };

    return payload;
  } catch (error) {
    console.error("Error in prepareCWpayload: ", error);
    throw error;
  }
}
