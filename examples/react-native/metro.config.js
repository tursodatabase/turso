const path = require('path');
const { getDefaultConfig, mergeConfig } = require('@react-native/metro-config');

const bindingsPath = path.resolve(__dirname, '../../bindings/react-native');

const config = {
  watchFolders: [bindingsPath],
  resolver: {
    extraNodeModules: {
      '@tursodatabase/react-native': bindingsPath,
    },
  },
};

module.exports = mergeConfig(getDefaultConfig(__dirname), config);
