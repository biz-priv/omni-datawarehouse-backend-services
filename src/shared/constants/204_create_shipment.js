'use strict';

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
      TableName: 'omni-wt-rt-confirmation-cost-dev',
      IndexName: 'omni-wt-confirmation-cost-orderNo-index-dev',
      KeyConditionExpression: 'FK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    }),
    tbl_Shipper: ({ orderNo }) => ({
      TableName: 'omni-wt-rt-shipper-dev',
      KeyConditionExpression: 'FK_ShipOrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    }),
    tbl_Consignee: ({ orderNo }) => ({
      TableName: 'omni-wt-rt-consignee-dev',
      KeyConditionExpression: 'FK_ConOrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    }),
    tbl_ShipmentHeader: ({ orderNo }) => ({
      TableName: 'omni-wt-rt-shipment-header-dev',
      KeyConditionExpression: 'PK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    }),
    tbl_TrackingNotes: ({ consoleNo }) => ({
      TableName: 'omni-wt-rt-tracking-notes-dev',
      IndexName: 'omni-tracking-notes-console-index-dev',
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      ExpressionAttributeValues: {
        ':ConsolNo': consoleNo,
      },
    }),
  },
  [TYPES.NON_CONSOLE]: {
    tbl_ConfirmationCost: ({ orderNo }) => ({
      TableName: 'omni-wt-rt-confirmation-cost-dev',
      IndexName: 'omni-wt-confirmation-cost-orderNo-index-dev',
      KeyConditionExpression: 'FK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    }),
    tbl_Shipper: ({ orderNo }) => ({
      TableName: 'omni-wt-rt-shipper-dev',
      KeyConditionExpression: 'FK_ShipOrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    }),
    tbl_Consignee: ({ orderNo }) => ({
      TableName: 'omni-wt-rt-consignee-dev',
      KeyConditionExpression: 'FK_ConOrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    }),
    tbl_ShipmentHeader: ({ orderNo }) => ({
      TableName: 'omni-wt-rt-shipment-header-dev',
      KeyConditionExpression: 'PK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    }),
  },
  [TYPES.MULTI_STOP]: {
    tbl_ShipmentHeader: ({ orderNo }) => ({
      TableName: 'omni-wt-rt-shipment-header-dev',
      KeyConditionExpression: 'PK_OrderNo = :PK_OrderNo',
      ExpressionAttributeValues: {
        ':PK_OrderNo': orderNo,
      },
    }),
    tbl_ConsolStopItems: ({ orderNo }) => ({
      TableName: 'omni-wt-rt-consol-stop-items-dev',
      KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
      ExpressionAttributeValues: {
        ':FK_OrderNo': orderNo,
      },
    }),
    tbl_References: ({ orderNo }) => ({
      TableName: 'omni-wt-rt-references-dev',
      IndexName: 'omni-wt-rt-ref-orderNo-index-dev',
      KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
      ExpressionAttributeValues: {
        ':FK_OrderNo': orderNo,
      },
    }),
    tbl_ShipmentDesc: ({ orderNo }) => ({
      TableName: 'omni-wt-rt-shipment-desc-dev',
      KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
      ExpressionAttributeValues: {
        ':FK_OrderNo': orderNo,
      },
    }),
    tbl_TrackingNotes: ({ consoleNo }) => ({
      TableName: 'omni-wt-rt-tracking-notes-dev',
      IndexName: 'omni-tracking-notes-console-index-dev',
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      ExpressionAttributeValues: {
        ':ConsolNo': consoleNo,
      },
    }),
  },
};

const VENDOR = 'LIVELOGI';

module.exports = { STATUSES, TYPES, CONSOLE_WISE_TABLES, VENDOR, TABLE_PARAMS };
