import dataApi from './data';
import { randomString, sampleConsoleLog } from './utils';

const doDryRun = process.argv[2] === '--dry-run';

const main = async () => {
  const startDate = new Date();
  const logKey = randomString(12);
  console.log(`(${logKey}) clean starts on ${startDate.toISOString()}`);
  console.log(`(${logKey}) doDryRun: ${doDryRun}`);

  const logs = await dataApi.getObsoleteFileWorkLogs();
  sampleConsoleLog(logs);
  console.log(`(${logKey}) Got ${logs.length} entities`);
  if (logs.length > 0) {
    if (!doDryRun) await dataApi.deleteFileWorkLogs(logs);
    console.log(`(${logKey}) Deleted the entities`);
  }
};

main();
