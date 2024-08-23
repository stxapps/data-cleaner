import { Datastore, PropertyFilter, and } from '@google-cloud/datastore';
import { Storage } from '@google-cloud/storage';

import {
  VERIFY_LOG, NOTIFY_LOG, ACKNOWLEDGE_LOG, EXTRACT, EXTRACT_LOG, BACKUP_BUCKET,
  FILE_INFO, FILE_LOG, FILE_WORK_LOG, DELETED,
} from './const';

const datastore = new Datastore();
const storage = new Storage();

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

const deleteFiles = async (bucketName, paths, nItems = 32) => {
  const bucket = storage.bucket(bucketName);
  for (let i = 0; i < paths.length; i += nItems) {
    const selectedPaths = paths.slice(i, i + nItems);
    await Promise.all(
      selectedPaths.map(path => {
        const bucketFile = bucket.file(path);
        return bucketFile.delete().catch(error => {
          console.log(`In deleteFiles, ${path} has error:`, error);
        });
      })
    );
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

const getDeletedFileInfos = async () => {
  const dt = Date.now() - (31 * 24 * 60 * 60 * 1000);
  const date = new Date(dt);

  const query = datastore.createQuery(FILE_INFO);
  query.filter(and([
    new PropertyFilter('status', '=', DELETED),
    new PropertyFilter('updateDate', '<', date),
  ])); // Need Composite Index Configuration in index.yaml in sdrive-hub
  query.order('updateDate', { descending: false });
  query.limit(2400);

  const entities = await queryData(query, true);

  const infos = [];
  for (const entity of entities) {
    const info = {
      path: entity[datastore.KEY].name,
      status: entity.status,
      size: entity.size,
      createDate: entity.createDate,
      updateDate: entity.updateDate,
    };
    infos.push(info);
  }
  return infos;
};

/*const getObsoleteSDriveHub = async () => {
  // BucketInfos, FileInfos
};*/

const deleteSDriveHubBackUp = async (infos) => {
  const paths = [];
  for (const info of infos) {
    paths.push(info.path);
  }

  await deleteFiles(BACKUP_BUCKET, paths, 80);
};

/*const deleteBucketInfos = async () => {

};*/

const deleteFileInfos = async (infos) => {
  const keys = [];
  for (const info of infos) {
    keys.push(datastore.key([FILE_INFO, info.path]));
  }

  await deleteData(keys, 200);
};

const getObsoleteFileLogs = async () => {
  const dt = Date.now() - (31 * 24 * 60 * 60 * 1000);
  const date = new Date(dt);

  const query = datastore.createQuery(FILE_LOG);
  query.filter(new PropertyFilter('createDate', '<', date));
  query.order('createDate', { descending: false });
  query.limit(20480);

  const entities = await queryData(query, true);

  const logs = [];
  for (const entity of entities) {
    const log = {
      name: entity[datastore.KEY].name,
      createDate: entity.createDate,
    };
    logs.push(log);
  }
  return logs;
};

const deleteFileLogs = async (logs) => {
  const keys = [];
  for (const log of logs) {
    keys.push(datastore.key([FILE_LOG, log.name]));
  }

  await deleteData(keys, 1024);
};

const getObsoleteFileWorkLogs = async () => {
  const dt = Date.now() - (31 * 24 * 60 * 60 * 1000);
  const date = new Date(dt);

  const query = datastore.createQuery(FILE_WORK_LOG);
  query.filter(new PropertyFilter('createDate', '<', date));
  query.order('createDate', { descending: false });
  query.limit(752);

  const entities = await queryData(query, true);

  const logs = [];
  for (const entity of entities) {
    const log = {
      id: entity[datastore.KEY].id,
      createDate: entity.createDate,
    };
    logs.push(log);
  }
  return logs;
};

const deleteFileWorkLogs = async (logs) => {
  const keys = [];
  for (const log of logs) {
    keys.push(datastore.key([FILE_WORK_LOG, datastore.int(log.id)]));
  }

  await deleteData(keys, 256);
};

const data = {
  getObsoleteVerifyLogs, deleteVerifyLogs, getObsoleteNotifyLogs, deleteNotifyLogs,
  getObsoleteAcknowledgeLogs, deleteAcknowledgeLogs, getObsoleteBraceStaticFiles,
  deleteBraceStaticFiles, getObsoleteExtracts, deleteExtracts, getObsoleteExtractLogs,
  deleteExtractLogs, getDeletedFileInfos, deleteSDriveHubBackUp, deleteFileInfos,
  getObsoleteFileLogs, deleteFileLogs, getObsoleteFileWorkLogs, deleteFileWorkLogs,
};

export default data;
