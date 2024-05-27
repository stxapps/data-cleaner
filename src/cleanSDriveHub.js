import { randomString } from './utils';

const doDryRun = process.argv[2] === '--dry-run';

const main = async () => {
  const startDate = new Date();
  const logKey = randomString(12);
  console.log(`(${logKey}) clean starts on ${startDate.toISOString()}`);
  console.log(`(${logKey}) doDryRun: ${doDryRun}`);

  console.log('Not implemented.');
};

main();
