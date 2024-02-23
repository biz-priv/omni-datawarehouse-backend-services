'use strict';

const {
  SHIPMENT_HEADER_TABLE,
  SHIPMENT_DESC_TABLE,
  CONSOL_STOP_ITEMS,
  CONFIRMATION_COST,
  CONFIRMATION_COST_INDEX_KEY_NAME,
  CONSIGNEE_TABLE,
  SHIPPER_TABLE,
  REFERENCES_TABLE,
  TRACKING_NOTES_TABLE,
  TRACKING_NOTES_CONSOLENO_INDEX_KEY,
  CONSOL_STOP_HEADERS_CONSOL_INDEX,
  CONSOL_STOP_HEADERS,
} = process.env;

const STATUSES = {
  PENDING: 'PENDING',
  READY: 'READY',
  SENT: 'SENT',
  FAILED: 'FAILED',
  SKIPPED: 'SKIPPED',
};

const TYPES = {
  CONSOLE: 'CONSOLE',
  NON_CONSOLE: 'NON_CONSOLE',
  MULTI_STOP: 'MULTI_STOP',
};

const CONSOLE_WISE_TABLES = {
  [TYPES.CONSOLE]: {
    tbl_ConfirmationCost: STATUSES.PENDING,
    tbl_TrackingNotes: STATUSES.PENDING,
    tbl_ShipmentDesc: STATUSES.PENDING,
  },
  [TYPES.NON_CONSOLE]: {
    tbl_ConfirmationCost: STATUSES.PENDING,
    tbl_Shipper: STATUSES.PENDING,
    tbl_Consignee: STATUSES.PENDING,
    tbl_ShipmentHeader: STATUSES.PENDING,
    tbl_ShipmentDesc: STATUSES.PENDING,
  },
  [TYPES.MULTI_STOP]: {
    tbl_ShipmentHeader: STATUSES.PENDING,
    tbl_ConsolStopItems: STATUSES.PENDING,
    tbl_ShipmentDesc: STATUSES.PENDING,
    tbl_TrackingNotes: STATUSES.PENDING,
    tbl_References: STATUSES.PENDING,
    tbl_ConsolStopHeaders: STATUSES.PENDING,
  },
};

const TABLE_PARAMS = {
  [TYPES.CONSOLE]: {
    tbl_ConfirmationCost: ({ orderNo }) => ({
      TableName: CONFIRMATION_COST,
      IndexName: CONFIRMATION_COST_INDEX_KEY_NAME,
      KeyConditionExpression: 'FK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    }),
    tbl_TrackingNotes: ({ consoleNo }) => ({
      TableName: TRACKING_NOTES_TABLE,
      IndexName: TRACKING_NOTES_CONSOLENO_INDEX_KEY,
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      ExpressionAttributeValues: {
        ':ConsolNo': String(consoleNo),
      },
    }),
    tbl_ShipmentDesc: ({ orderNo }) => ({
      TableName: SHIPMENT_DESC_TABLE,
      KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
      ExpressionAttributeValues: {
        ':FK_OrderNo': orderNo,
      },
    }),
  },
  [TYPES.NON_CONSOLE]: {
    tbl_ConfirmationCost: ({ orderNo }) => ({
      TableName: CONFIRMATION_COST,
      IndexName: CONFIRMATION_COST_INDEX_KEY_NAME,
      KeyConditionExpression: 'FK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    }),
    tbl_Shipper: ({ orderNo }) => ({
      TableName: SHIPPER_TABLE,
      KeyConditionExpression: 'FK_ShipOrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    }),
    tbl_Consignee: ({ orderNo }) => ({
      TableName: CONSIGNEE_TABLE,
      KeyConditionExpression: 'FK_ConOrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    }),
    tbl_ShipmentHeader: ({ orderNo }) => ({
      TableName: SHIPMENT_HEADER_TABLE,
      KeyConditionExpression: 'PK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    }),
    tbl_ShipmentDesc: ({ orderNo }) => ({
      TableName: SHIPMENT_DESC_TABLE,
      KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
      ExpressionAttributeValues: {
        ':FK_OrderNo': orderNo,
      },
    }),
  },
  [TYPES.MULTI_STOP]: {
    tbl_ShipmentHeader: ({ orderNo }) => ({
      TableName: SHIPMENT_HEADER_TABLE,
      KeyConditionExpression: 'PK_OrderNo = :PK_OrderNo',
      ExpressionAttributeValues: {
        ':PK_OrderNo': orderNo,
      },
    }),
    tbl_ConsolStopItems: ({ orderNo }) => ({
      TableName: CONSOL_STOP_ITEMS,
      KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
      ExpressionAttributeValues: {
        ':FK_OrderNo': orderNo,
      },
    }),
    tbl_References: ({ orderNo }) => ({
      TableName: REFERENCES_TABLE,
      IndexName: 'omni-wt-rt-ref-orderNo-index-dev',
      KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
      ExpressionAttributeValues: {
        ':FK_OrderNo': orderNo,
      },
    }),
    tbl_ShipmentDesc: ({ orderNo }) => ({
      TableName: SHIPMENT_DESC_TABLE,
      KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
      ExpressionAttributeValues: {
        ':FK_OrderNo': orderNo,
      },
    }),
    tbl_TrackingNotes: ({ consoleNo }) => ({
      TableName: TRACKING_NOTES_TABLE,
      IndexName: TRACKING_NOTES_CONSOLENO_INDEX_KEY,
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      ExpressionAttributeValues: {
        ':ConsolNo': String(consoleNo),
      },
    }),
    tbl_ConsolStopHeaders: ({ consoleNo }) => ({
      TableName: CONSOL_STOP_HEADERS,
      IndexName: CONSOL_STOP_HEADERS_CONSOL_INDEX,
      KeyConditionExpression: 'FK_ConsolNo = :ConsolNo',
      ExpressionAttributeValues: {
        ':ConsolNo': String(consoleNo),
      },
    }),
  },
};

const CONSOLE_WISE_REQUIRED_FIELDS = {
  [TYPES.CONSOLE]: {
    tbl_ConfirmationCost: [
      'Consignee Name',
      'Consignee Address1',
      'Consignee City',
      'Consignee State',
      'Consignee Country',
      'Shipper Address1',
      'Shipper City',
      'Shipper Name',
      'Shipper State',
      'Shipper Country',
    ],
    tbl_TrackingNotes: ['UserId'],
    tbl_ShipmentDesc: ['Hazmat', 'Pieces', 'Weight'],
  },
  [TYPES.NON_CONSOLE]: {
    tbl_ConfirmationCost: [
      'Consignee Name',
      'Consignee Address1',
      'Consignee City',
      'Consignee State',
      'Consignee Country',
      'Shipper Address1',
      'Shipper City',
      'Shipper Name',
      'Shipper State',
      'Shipper Country',
    ],
    tbl_Shipper: [
      'Shipper Address1',
      'Shipper City',
      'Shipper Name',
      'Shipper State',
      'Shipper Country',
    ],
    tbl_Consignee: [
      'Consignee Name',
      'Consignee Address1',
      'Consignee City',
      'Consignee State',
      'Consignee Country',
    ],
    tbl_ShipmentHeader: [
      'EquipmentCode',
      'Insurance',
      'ServiceLevelId',
      'OrderDate',
      'ReadyDateTime',
      'ReadyDateTimeRange',
      'ScheduledDateTime',
      'ScheduledDateTimeRange',
    ],
    tbl_ShipmentDesc: ['Hazmat', 'Pieces', 'Weight'],
  },
  [TYPES.MULTI_STOP]: {
    tbl_ShipmentHeader: ['EquipmentCode', 'Insurance', 'ServiceLevelId', 'Housebill'],
    tbl_ShipmentDesc: ['Hazmat', 'Pieces', 'Weight'],
    tbl_TrackingNotes: ['UserId'],
    tbl_References: ['Reference', 'Reference type'],
    tbl_ConsolStopHeaders: [
      'Consol Number',
      'ConsolStop Name',
      'ConsolStop Address1',
      'ConsolStop Address2',
      'ConsolStop City',
      'ConsolStop State',
      'ConsolStop TimeBegin',
      'ConsolStop TimeEnd',
      'ConsolStop Date',
    ],
  },
};

const VENDOR = 'LIVELOGI';

module.exports = {
  STATUSES,
  TYPES,
  CONSOLE_WISE_TABLES,
  VENDOR,
  TABLE_PARAMS,
  CONSOLE_WISE_REQUIRED_FIELDS,
};
