import { useEffect, useState } from 'react';
import {
  displayResults, runTests,
} from '@op-engineering/op-test';
import './tests'; // import all tests to register them
import { SafeAreaProvider, SafeAreaView } from 'react-native-safe-area-context';
import { performanceTest } from './performance_test';
import { StyleSheet, Text, View } from 'react-native';
import { connect, setup } from '@tursodatabase/react-native';

export default function App() {
  const [results, setResults] = useState<any>(null);
  const [perfResult, setPerfResult] = useState<number>(0);
  const [openTime, setOpenTime] = useState(0);

  useEffect(() => {
    console.log("App has started 🟢")
    const work = async () => {
      // Configure Turso logging
      try {
        setup({ logLevel: 'debug' });
        console.log("Turso logging configured ✅")
      } catch (e) {
        console.log(`Failed to configure Turso logging: ${e}`)
      }

      let start = performance.now();
      await connect({ path: 'dummyDb.sqlite' });
      setOpenTime(performance.now() - start);

      try {
        const results = await runTests();
        console.log("TESTS FINISHED 🟢")
        setResults(results);
      } catch (e) {
        console.log(`TEST FAILED 🟥 ${e}`)
      }

      setTimeout(async () => {
        try {
          global?.gc?.();
          let perfRes = await performanceTest();
          setPerfResult(perfRes);
        } catch (e) {
          // intentionally left blank
        }
      }, 1000);
    };

    work();
  }, []);

  return (
    <SafeAreaProvider>
      <SafeAreaView style={styles.container}>
        <View>
          <Text style={styles.performanceText}>
            Open DB time: {openTime.toFixed(0)} ms
          </Text>
          <Text style={styles.performanceText}>
            1_000 query time: {perfResult.toFixed(0)} ms
          </Text>
        </View>
        <View style={styles.results}>{displayResults(results)}</View>
      </SafeAreaView>
    </SafeAreaProvider>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#222',
    gap: 4,
    padding: 10,
  },
  results: {
    flex: 1,
  },
  performanceText: {
    color: 'white',
    fontSize: 16,
  },
});
