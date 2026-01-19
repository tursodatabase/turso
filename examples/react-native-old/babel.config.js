const path = require('path');
const { getConfig } = require('react-native-builder-bob/babel-config');
const pkg = require('../../bindings/react-native/package.json');

const root = path.resolve(__dirname, '../../bindings/react-native');

module.exports = getConfig(
  {
    presets: ['module:@react-native/babel-preset'],
  },
  { root, pkg },
);
