import dataApi from './data';
import { randomString, sampleConsoleLog } from './utils';

const doDryRun = process.argv[2] === '--dry-run';

const main = async () => {
  const startDate = new Date();
  const logKey = randomString(12);
  console.log(`(${logKey}) clean starts on ${startDate.toISOString()}`);
  console.log(`(${logKey}) doDryRun: ${doDryRun}`);

  const infos = await dataApi.getDeletedFileInfos();
  sampleConsoleLog(infos)
  console.log(`(${logKey}) Got ${infos.length} entities`);
  if (infos.length > 0) {
    if (!doDryRun) await dataApi.deleteSDriveHubBackUp(infos);
    console.log(`(${logKey}) Deleted in the backup bucket`);

    if (!doDryRun) await dataApi.deleteFileInfos(infos);
    console.log(`(${logKey}) Deleted the entities`);
  }
};

main();
