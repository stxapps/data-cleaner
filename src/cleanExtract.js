import dataApi from './data';
import { randomString, sampleConsoleLog } from './utils';

const doDryRun = process.argv[2] === '--dry-run';

const main = async () => {
  const startDate = new Date();
  const logKey = randomString(12);
  console.log(`(${logKey}) clean starts on ${startDate.toISOString()}`);
  console.log(`(${logKey}) doDryRun: ${doDryRun}`);

  const extracts = await dataApi.getObsoleteExtracts();
  sampleConsoleLog(extracts);
  console.log(`(${logKey}) Got ${extracts.length} entities`);
  if (extracts.length > 0) {
    if (!doDryRun) await dataApi.deleteExtracts(extracts);
    console.log(`(${logKey}) Deleted the entities`);
  }
};

main();
