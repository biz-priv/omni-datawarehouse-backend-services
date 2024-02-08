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
} = process.env;

const STATUSES = {
  PENDING: 'PENDING',
  READY: 'READY',
  SENT: 'SENT',
  FAILED: 'FAILED',
};

const TYPES = {
  CONSOLE: 'CONSOLE',
  NON_CONSOLE: 'NON_CONSOLE',
  MULTI_STOP: 'MULTI_STOP',
};

const CONSOLE_WISE_TABLES = {
  [TYPES.CONSOLE]: {
    tbl_ConfirmationCost: STATUSES.PENDING,
    tbl_Shipper: STATUSES.PENDING,
    tbl_Consignee: STATUSES.PENDING,
    tbl_ShipmentHeader: STATUSES.PENDING,
    tbl_TrackingNotes: STATUSES.PENDING,
  },
  [TYPES.NON_CONSOLE]: {
    tbl_ConfirmationCost: STATUSES.PENDING,
    tbl_Shipper: STATUSES.PENDING,
    tbl_Consignee: STATUSES.PENDING,
    tbl_ShipmentHeader: STATUSES.PENDING,
  },
  [TYPES.MULTI_STOP]: {
    tbl_ShipmentHeader: STATUSES.PENDING,
    tbl_ConsolStopItems: STATUSES.PENDING,
    tbl_ShipmentDesc: STATUSES.PENDING,
    tbl_TrackingNotes: STATUSES.PENDING,
    tbl_References: STATUSES.PENDING,
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
    tbl_TrackingNotes: ({ consoleNo }) => ({
      TableName: TRACKING_NOTES_TABLE,
      IndexName: TRACKING_NOTES_CONSOLENO_INDEX_KEY,
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      ExpressionAttributeValues: {
        ':ConsolNo': String(consoleNo),
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
  },
};

const VENDOR = 'LIVELOGI';

module.exports = { STATUSES, TYPES, CONSOLE_WISE_TABLES, VENDOR, TABLE_PARAMS };
