export const isObject = (val) => {
  return typeof val === 'object' && val !== null;
};

export const isString = (val) => {
  return typeof val === 'string' || val instanceof String;
};

export const isNumber = val => {
  return typeof val === 'number' && isFinite(val);
};

export const randomString = (length) => {
  const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
  const charactersLength = characters.length;

  let result = '';
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
};

export const extractPath = (path) => {
  const arr = path.split('/');
  if (arr.length < 1) console.log(`In extractPath, invalid path: ${path}`);

  const address = arr[0] || '';

  return { address };
};

export const sampleConsoleLog = (values) => {
  for (const value of values.slice(0, 3)) {
    console.log(value);
  }
  if (values.length > 3) {
    console.log('.\n.\n.');
    for (const value of values.slice(Math.max(values.length - 3, 3), values.length)) {
      console.log(value);
    }
  }
};
