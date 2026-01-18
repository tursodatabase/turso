const {getDefaultConfig, mergeConfig} = require('@react-native/metro-config');
const path = require('path');

const defaultConfig = getDefaultConfig(__dirname);

// Watch the turso-react-native package
const tursoPath = path.resolve(__dirname, '../../bindings/react-native');

const config = {
  watchFolders: [tursoPath],
  resolver: {
    nodeModulesPaths: [
      path.resolve(__dirname, 'node_modules'),
      path.resolve(tursoPath, 'node_modules'),
    ],
  },
};

module.exports = mergeConfig(defaultConfig, config);
