module.exports = {
  presets: ['module:@react-native/babel-preset'],
  plugins: [
    ['transform-inline-environment-variables', {
      include: [
        'TURSO_DATABASE_URL',
        'TURSO_AUTH_TOKEN',
        'TURSO_ENCRYPTION_KEY',
        'TURSO_ENCRYPTION_CIPHER',
      ],
    }],
  ],
};
