import dataApi from './data';
import { randomString } from './utils';

const doDryRun = process.argv[2] === '--dry-run';

const main = async () => {
  const startDate = new Date();
  const logKey = randomString(12);
  console.log(`(${logKey}) clean starts on ${startDate.toISOString()}`);
  console.log(`(${logKey}) doDryRun: ${doDryRun}`);

  const logs = await dataApi.getObsoleteNotifyLogs();
  if (logs.length > 0) {
    for (const log of logs) console.log(log);
  }
  console.log(`(${logKey}) Got ${logs.length} entities`);
  if (logs.length > 0) {
    if (!doDryRun) await dataApi.deleteNotifyLogs(logs);
    console.log(`(${logKey}) Deleted the entities`);
  }
};

main();
