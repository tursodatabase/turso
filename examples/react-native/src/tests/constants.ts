import {
  IOS_DOCUMENT_PATH,
  IOS_LIBRARY_PATH,
  ANDROID_EXTERNAL_FILES_PATH,
  ANDROID_DATABASE_PATH,
  ANDROID_FILES_PATH,
} from '@tursodatabase/react-native';
import {describe, it, expect} from '@op-engineering/op-test';
import {Platform} from 'react-native';

describe('Constants tests', () => {
  it('Constants are there', async () => {
    if (Platform.OS === 'ios') {
      expect(!!IOS_DOCUMENT_PATH).toBeTruthy();
      expect(!!IOS_LIBRARY_PATH).toBeTruthy();
    } else {
      expect(!!ANDROID_EXTERNAL_FILES_PATH).toBeTruthy();
      expect(!!ANDROID_DATABASE_PATH).toBeTruthy();
      expect(!!ANDROID_FILES_PATH).toBeTruthy();
    }
  });
});
