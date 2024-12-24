import { Datastore, PropertyFilter } from '@google-cloud/datastore';

import { VERIFY_LOG, NOTIFY_LOG, ACKNOWLEDGE_LOG, EXTRACT, EXTRACT_LOG } from './const';

const datastore = new Datastore();

const queryData = async (query, readOnly) => {
  const transaction = datastore.transaction({ readOnly });
  try {
    await transaction.run();
    const [entities] = await transaction.runQuery(query);
    await transaction.commit();
    return entities;
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
};

const deleteData = async (keys, nKeys = 64) => {
  for (let i = 0; i < keys.length; i += nKeys) {
    const selectedKeys = keys.slice(i, i + nKeys);

    const transaction = datastore.transaction();
    try {
      await transaction.run();

      transaction.delete(selectedKeys);
      await transaction.commit();
    } catch (e) {
      await transaction.rollback();
      throw e;
    }
  }
};

const getObsoleteVerifyLogs = async () => {
  const dt = Date.now() - (90 * 24 * 60 * 60 * 1000);
  const date = new Date(dt);

  const query = datastore.createQuery(VERIFY_LOG);
  query.filter(new PropertyFilter('updateDate', '<', date));
  query.order('updateDate', { descending: false });
  query.limit(64);

  const entities = await queryData(query, true);

  const logs = [];
  for (const entity of entities) {
    const log = {
      logKey: entity.logKey,
      id: entity[datastore.KEY].id,
      productId: entity.productId,
      source: entity.source,
      userId: entity.userId,
      updateDate: entity.updateDate,
    };
    logs.push(log);
  }
  return logs;
};

const deleteVerifyLogs = async (logs) => {
  const keys = [];
  for (const log of logs) {
    keys.push(datastore.key([VERIFY_LOG, datastore.int(log.id)]));
  }

  await deleteData(keys);
};

const getObsoleteNotifyLogs = async () => {
  const dt = Date.now() - (90 * 24 * 60 * 60 * 1000);
  const date = new Date(dt);

  const query = datastore.createQuery(NOTIFY_LOG);
  query.filter(new PropertyFilter('updateDate', '<', date));
  query.order('updateDate', { descending: false });
  query.limit(64);

  const entities = await queryData(query, true);

  const logs = [];
  for (const entity of entities) {
    const log = {
      logKey: entity.logKey,
      id: entity[datastore.KEY].id,
      source: entity.source,
      originalOrderId: entity.originalOrderId,
      updateDate: entity.updateDate,
    };
    logs.push(log);
  }
  return logs;
};

const deleteNotifyLogs = async (logs) => {
  const keys = [];
  for (const log of logs) {
    keys.push(datastore.key([NOTIFY_LOG, datastore.int(log.id)]));
  }

  await deleteData(keys);
};

const getObsoleteAcknowledgeLogs = async () => {
  const dt = Date.now() - (90 * 24 * 60 * 60 * 1000);
  const date = new Date(dt);

  const query = datastore.createQuery(ACKNOWLEDGE_LOG);
  query.filter(new PropertyFilter('updateDate', '<', date));
  query.order('updateDate', { descending: false });
  query.limit(128);

  const entities = await queryData(query, true);

  const logs = [];
  for (const entity of entities) {
    const log = {
      logKey: entity.logKey,
      id: entity[datastore.KEY].id,
      productId: entity.productId,
      acknowledgeResult: entity.acknowledgeResult,
      acknowledgeState: entity.acknowledgeState,
      updateDate: entity.updateDate,
    };
    logs.push(log);
  }
  return logs;
};

const deleteAcknowledgeLogs = async (logs) => {
  const keys = [];
  for (const log of logs) {
    keys.push(datastore.key([ACKNOWLEDGE_LOG, datastore.int(log.id)]));
  }

  await deleteData(keys);
};

const getObsoleteBraceStaticFiles = async () => {
  // Not implemented.
};

const deleteBraceStaticFiles = async () => {
  // Call deleteFiles. Not implemented.
};

const getExtractEntities = async (status) => {
  const query = datastore.createQuery(EXTRACT);
  query.filter(new PropertyFilter('status', '=', status));
  query.limit(10000);

  const entities = await queryData(query, true);

  const extracts = [];
  for (const entity of entities) {
    const extract = {
      name: entity[datastore.KEY].name,
      status: entity.status,
      extractDate: entity.extractDate,
    };
    extracts.push(extract);
  }
  return extracts;
};

const getObsoleteExtracts = async () => {
  const dt = Date.now() - (365 * 24 * 60 * 60 * 1000);
  const date = new Date(dt);

  const query = datastore.createQuery(EXTRACT);
  query.filter(new PropertyFilter('extractDate', '<', date));
  query.order('extractDate', { descending: false });
  query.limit(2048);

  const entities = await queryData(query, true);

  const extracts = [];
  for (const entity of entities) {
    const extract = {
      name: entity[datastore.KEY].name,
      status: entity.status,
      extractDate: entity.extractDate,
    };
    extracts.push(extract);
  }
  return extracts;
};

const deleteExtracts = async (extracts) => {
  const keys = [];
  for (const extract of extracts) {
    keys.push(datastore.key([EXTRACT, extract.name]));
  }

  await deleteData(keys, 1024);
};

const getObsoleteExtractLogs = async () => {
  const dt = Date.now() - (90 * 24 * 60 * 60 * 1000);
  const date = new Date(dt);

  const query = datastore.createQuery(EXTRACT_LOG);
  query.filter(new PropertyFilter('endDate', '<', date));
  query.order('endDate', { descending: false });
  query.limit(752);

  const entities = await queryData(query, true);

  const logs = [];
  for (const entity of entities) {
    const log = {
      id: entity[datastore.KEY].id,
      nEntities: entity.nEntities,
      nSuccesses: entity.nSuccesses,
      endDate: entity.endDate,
    };
    logs.push(log);
  }
  return logs;
};

const deleteExtractLogs = async (logs) => {
  const keys = [];
  for (const log of logs) {
    keys.push(datastore.key([EXTRACT_LOG, datastore.int(log.id)]));
  }

  await deleteData(keys, 256);
};

const data = {
  getObsoleteVerifyLogs, deleteVerifyLogs, getObsoleteNotifyLogs, deleteNotifyLogs,
  getObsoleteAcknowledgeLogs, deleteAcknowledgeLogs, getObsoleteBraceStaticFiles,
  deleteBraceStaticFiles, getExtractEntities, getObsoleteExtracts, deleteExtracts,
  getObsoleteExtractLogs, deleteExtractLogs,
};

export default data;
