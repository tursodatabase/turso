use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use std::sync::Arc;
use turso_core::{Database, PlatformIO};

// Title: JSONB Function Benchmarking

fn rusqlite_open() -> rusqlite::Connection {
    let sqlite_conn = rusqlite::Connection::open("../testing/testing.db").unwrap();
    sqlite_conn
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    sqlite_conn
}

fn bench(criterion: &mut Criterion) {
    // Flag to disable rusqlite benchmarks if needed
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false, false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Benchmark JSONB with different payload sizes
    let json_sizes = [
        ("Small", r#"{"id": 1, "name": "Test"}"#),
        (
            "Medium",
            r#"{"id": 1, "name": "Test", "attributes": {"color": "blue", "size": "medium", "tags": ["tag1", "tag2", "tag3"]}}"#,
        ),
        (
            "Large",
            r#"[{"metadata":{"title":"Standard JSON Test File","description":"A complex JSON file for testing parsers and serializers (Standard JSON only)","version":"1.0.0","generated":"2025-03-12T12:00:00Z","author":"Claude AI"},"primitives":{"null_value":null,"boolean_values":{"true_value":true,"false_value":false},"number_values":{"integer":42,"negative":-273,"zero":0,"large_integer":9007199254740991,"small_integer":-9007199254740991,"decimal":3.14159265358979,"negative_decimal":-2.71828,"exponent_positive":6.022e+23,"exponent_negative":1.602e-19},"string_values":{"empty":"","simple":"Hello, world!","unicode":"你好，世界！😀🌍🚀","quotes":"She said \"Hello!\" to me.","backslash":"C:\\Program Files\\App\\","controls":"Line1\nLine2\tTabbed\rCarriage\bBackspace\fForm-feed","unicode_escapes":"Copyright: ©, Emoji: 😀","all_escapes":"\\b\\f\\n\\r\\t\\\"\\\\"}},"arrays":{"empty_array":[],"homogeneous":[1,2,3,4,5,6,7,8,9,10],"heterogeneous":[null,true,42,"string",{"key":"value"},[1,2,3]],"nested":[[1,2,3],[4,5,6],[7,8,9]],"deep":[[[[[[[[[["Very deep"]]]]]]]]]],"large":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99]},"objects":{"empty_object":{},"simple_object":{"key1":"value1","key2":"value2"},"nested_object":{"level1":{"level2":{"level3":{"level4":{"level5":"Deep nesting"}}}}},"complex_keys":{"simple":"value","with spaces":"value","with-dash":"value","with_underscore":"value","with.dot":"value","with:colon":"value","with@symbol":"value","withUnicode":"value","withEmoji":"value","withQuotes":"value","withBackslashes":"value"}},"edge_cases":{"zero_byte_string":"","one_byte_string":"x","long_string":"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.","almost_too_deep":{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":{"k":{"l":{"m":{"n":{"o":{"p":{"q":{"r":{"s":{"t":{"u":{"v":{"w":{"x":{"y":{"z":"Deep nesting test"}}}}}}}}}}}}}}}}}}}}}}}}}}},"many_properties":{"prop01":1,"prop02":2,"prop03":3,"prop04":4,"prop05":5,"prop06":6,"prop07":7,"prop08":8,"prop09":9,"prop10":10,"prop11":11,"prop12":12,"prop13":13,"prop14":14,"prop15":15,"prop16":16,"prop17":17,"prop18":18,"prop19":19,"prop20":20,"prop21":21,"prop22":22,"prop23":23,"prop24":24,"prop25":25,"prop26":26,"prop27":27,"prop28":28,"prop29":29,"prop30":30,"prop31":31,"prop32":32,"prop33":33,"prop34":34,"prop35":35,"prop36":36,"prop37":37,"prop38":38,"prop39":39,"prop40":40,"prop41":41,"prop42":42,"prop43":43,"prop44":44,"prop45":45,"prop46":46,"prop47":47,"prop48":48,"prop49":49,"prop50":50}},{"standard_features":{"numeric_literals":{"decimal_integer":12345,"negative_integer":-12345,"decimal_fraction":123.45,"negative_fraction":-123.45,"exponential_positive":123400,"exponential_negative":0.00001234},"string_escapes":{"quotation_mark":"Quote: \"Hello\"","reverse_solidus":"Backslash: \\","solidus":"Slash: / (optional escape)","backspace":"Control: \b","formfeed":"Control: \f","newline":"Control: \n","carriage_return":"Control: \r","tab":"Control: \t","unicode":"Unicode: © € ☃"}},"generated_data":{"people":[{"id":1,"name":"John Smith","email":"john.smith@example.com","age":42,"address":{"street":"123 Main St","city":"Anytown","state":"CA","zip":"12345"},"phone_numbers":[{"type":"home","number":"555-1234"},{"type":"work","number":"555-5678"}],"tags":["employee","manager","developer"],"active":true},{"id":2,"name":"Jane Doe","email":"jane.doe@example.com","age":36,"address":{"street":"456 Elm St","city":"Othertown","state":"NY","zip":"67890"},"phone_numbers":[{"type":"mobile","number":"555-9012"}],"tags":["employee","designer"],"active":true},{"id":3,"name":"Bob Johnson","email":"bob.johnson@example.com","age":51,"address":{"street":"789 Oak St","city":"Somewhere","state":"TX","zip":"45678"},"phone_numbers":[{"type":"home","number":"555-3456"},{"type":"work","number":"555-7890"},{"type":"mobile","number":"555-1234"}],"tags":["employee","manager","sales"],"active":false}],"products":[{"id":"P001","name":"Smartphone","category":"Electronics","price":799.99,"features":["5G","Dual Camera","Fast Charging"],"specifications":{"dimensions":{"width":71.5,"height":146.7,"depth":7.4},"weight":174,"display":{"type":"OLED","size":6.1,"resolution":"1170x2532"},"processor":"A14 Bionic","memory":128},"in_stock":true,"release_date":"2023-09-15"},{"id":"P002","name":"Laptop","category":"Electronics","price":1299.99,"features":["16GB RAM","512GB SSD","Retina Display"],"specifications":{"dimensions":{"width":304.1,"height":212.4,"depth":15.6},"weight":1400,"display":{"type":"IPS","size":13.3,"resolution":"2560x1600"},"processor":"Intel Core i7","memory":512},"in_stock":true,"release_date":"2023-06-10"},{"id":"P003","name":"Wireless Headphones","category":"Audio","price":249.99,"features":["Noise Cancellation","20h Battery","Bluetooth 5.0"],"specifications":{"dimensions":{"width":168,"height":162,"depth":83},"weight":254,"driver":{"type":"Dynamic","size":40},"battery":{"capacity":500,"life":20}},"in_stock":false,"release_date":"2023-03-22"}],"orders":[{"id":"ORD-2023-001","customer_id":1,"date":"2023-01-15T10:30:00Z","items":[{"product_id":"P001","quantity":1,"price":799.99},{"product_id":"P003","quantity":2,"price":249.99}],"total":1299.97,"status":"delivered","shipping":{"address":{"street":"123 Main St","city":"Anytown","state":"CA","zip":"12345"},"method":"express","cost":15.99,"tracking_number":"SHP-123456789"},"payment":{"method":"credit_card","transaction_id":"TRX-987654321","status":"completed"}},{"id":"ORD-2023-002","customer_id":2,"date":"2023-02-20T14:45:00Z","items":[{"product_id":"P002","quantity":1,"price":1299.99}],"total":1299.99,"status":"shipped","shipping":{"address":{"street":"456 Elm St","city":"Othertown","state":"NY","zip":"67890"},"method":"standard","cost":9.99,"tracking_number":"SHP-234567890"},"payment":{"method":"paypal","transaction_id":"TRX-876543210","status":"completed"}},{"id":"ORD-2023-003","customer_id":3,"date":"2023-03-05T09:15:00Z","items":[{"product_id":"P001","quantity":1,"price":799.99},{"product_id":"P002","quantity":1,"price":1299.99},{"product_id":"P003","quantity":1,"price":249.99}],"total":2349.97,"status":"processing","shipping":{"address":{"street":"789 Oak St","city":"Somewhere","state":"TX","zip":"45678"},"method":"express","cost":25.99,"tracking_number":null},"payment":{"method":"bank_transfer","transaction_id":"TRX-765432109","status":"pending"}}]},"repeated_property_names":{"data":{"data":{"data":{"data":"Nested properties with the same name"}}}},"large_strings":{"base64_data":"iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAACXBIWXMAAAsTAAALEwEAmpwYAAAAIGNIUk0AAHolAACAgwAA+f8AAIDpAAB1MAAA6mAAADqYAAAXb5JfxUYAAAJ5SURBVHjalFJbSBRRGP7OzO7sbmrqbmo6lom5CYVEkiBIUKEPvRQFRlEEPvTSa0+9By+9BhFkIAhCQSJhWlEpaKVpZaSVpplpbre1vM26u7ObzpyZ0892MJMM6oOf833nO/f/ZwghWE86mOIq7YSQs3r0kqDgr8I2zlKb3k/G57fQz8Z9fOh6HDUsL9AcC5m2Zt4ZBBnLEbBCMfDNVv8s0T8f5sJaMCuW/51sDBQKYOcSUA68JgLhODzT8ToQh1ZeyhKtVMlgXl3NWZbDqcl3aOrtRFPvC/i5XJYk0UTjdiVduTuOleRCUomTiMdho+MxNvX1YlPfSwRMZthsdtllBUAIY27BkRWAbCx+jn+IQJB9NKwMQ0ehFYEVUIMQAh3DQKfRQqPTQR6FAPF4ImuYUQh7eGEuY+MJASMHg8kEvVYLPcsukBACVgiK9iS8ZtMBAQFDMeBp8NJQr9WAp03JOkgHBBZvitg8lkQRbr8fJJHMe5AgYj7iJYcPXPfNexf5rDFK0eFwQM+wYCgWLM2s1IWgYDqPrcVpXJZzUSY+Bp9t82wy6YG5sAJVlRtw0eWCw2RKt5oZiCQSYCkWT5bMOCK/AQgF1eofSDgOa4tKcKCuTnawGTFxLMrZGBpW38Md7SYI6Rts1lrZPRZ0g9d+w5LiiMx2lRVYjO15lTjbfw9sNIw9xdXYXVGBzTq99AVpcPncAz/f0cHnsojQaU3JF4jxhKaFQNUl4PJN4OJ5ICQAeg4jnvmMWWcnCErB5rHyD7Rmf3d1KbH+cOQx2XTkLJ5vPYL+8lrUL30FBjwYtDkw4nBmxNmKPwMAKrUbALKE+vwAAAAASUVORK5CYII=","lorem_ipsum":"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem. Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur? Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur, vel illum qui dolorem eum fugiat quo voluptas nulla pariatur?"},"binary_data_sizes":{"small_payload":{"description":"Small payload (0-11 bytes in header)","data":[1,2,3,4,5]},"medium_payload":{"description":"Medium payload (1-byte size in header)","data":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50]},"large_payload":{"description":"Simulation of large payload (2-byte size in header)","data_description":"Would normally contain 256-65535 bytes"},"extra_large_payload":{"description":"Simulation of extra large payload (4-byte size in header)","data_description":"Would normally contain >65535 bytes"}},"stress_test":{"recursive_structure":{"name":"Level 1","children":[{"name":"Level 1.1","children":[{"name":"Level 1.1.1","children":[]},{"name":"Level 1.1.2","children":[{"name":"Level 1.1.2.1","children":[]}]}]},{"name":"Level 1.2","children":[{"name":"Level 1.2.1","children":[]}]}]},"long_array_nested_objects":[{"id":1,"data":{"value":"test1"}},{"id":2,"data":{"value":"test2"}},{"id":3,"data":{"value":"test3"}},{"id":4,"data":{"value":"test4"}},{"id":5,"data":{"value":"test5"}},{"id":6,"data":{"value":"test6"}},{"id":7,"data":{"value":"test7"}},{"id":8,"data":{"value":"test8"}},{"id":9,"data":{"value":"test9"}},{"id":10,"data":{"value":"test10"}},{"id":11,"data":{"value":"test11"}},{"id":12,"data":{"value":"test12"}},{"id":13,"data":{"value":"test13"}},{"id":14,"data":{"value":"test14"}},{"id":15,"data":{"value":"test15"}},{"id":16,"data":{"value":"test16"}},{"id":17,"data":{"value":"test17"}},{"id":18,"data":{"value":"test18"}},{"id":19,"data":{"value":"test19"}},{"id":20,"data":{"value":"test20"}}]}}]"#,
        ), // Generate a larger JSON object with 20 nested items
        (
            "Real world json #1",
            r#"{
          "user": {
            "id": "usr_7f8d3a2e",
            "name": "Jane Smith",
            "email": "jane.smith@example.com",
            "verified": true,
            "created_at": "2023-05-12T15:42:31Z",
            "preferences": {
              "theme": "dark",
              "notifications": {
                "email": true,
                "push": false,
                "sms": true
              },
              "language": "en-US"
            },
            "subscription": {
              "plan": "premium",
              "status": "active",
              "next_billing_date": "2024-05-12"
            },
            "address": {
              "street": "123 Main St",
              "city": "Boston",
              "state": "MA",
              "zip": "02108",
              "country": "USA"
            }
          },
          "meta": {
            "request_id": "req_9d7e6c5b4a3f2e1d",
            "timestamp": 1683905123
          }
        }"#,
        ),
        (
            "Real world json 2",
            r#"{
          "products": [
            {
              "id": "p-1001",
              "name": "Wireless Headphones",
              "price": 79.99,
              "currency": "USD",
              "in_stock": true,
              "quantity": 45,
              "categories": ["electronics", "audio", "wireless"],
              "ratings": {
                "average": 4.7,
                "count": 238
              },
              "specs": {
                "brand": "SoundMax",
                "color": "black",
                "connectivity": "Bluetooth 5.0",
                "battery_life": "20 hours"
              },
              "images": [
                "https://example.com/products/headphones-1.jpg",
                "https://example.com/products/headphones-2.jpg"
              ]
            },
            {
              "id": "p-1002",
              "name": "Smart Watch",
              "price": 149.99,
              "currency": "USD",
              "in_stock": true,
              "quantity": 28,
              "categories": ["electronics", "wearables", "fitness"],
              "ratings": {
                "average": 4.3,
                "count": 182
              },
              "specs": {
                "brand": "TechFit",
                "color": "silver",
                "display": "AMOLED",
                "waterproof": true
              },
              "images": [
                "https://example.com/products/smartwatch-1.jpg",
                "https://example.com/products/smartwatch-2.jpg"
              ]
            }
          ],
          "pagination": {
            "total": 237,
            "page": 1,
            "per_page": 2,
            "next_page": 2
          }
        }
          "#,
        ),
        (
            "Real world json 3",
            r#"{
          "app_name": "DataProcessor",
          "version": "2.1.3",
          "environment": "production",
          "debug": false,
          "log_level": "info",
          "database": {
            "main": {
              "host": "db-primary.internal",
              "port": 5432,
              "name": "app_production",
              "user": "app_user",
              "max_connections": 50,
              "timeout_ms": 5000
            },
            "replica": {
              "host": "db-replica.internal",
              "port": 5432,
              "name": "app_production_replica",
              "user": "app_readonly",
              "max_connections": 25,
              "timeout_ms": 3000
            }
          },
          "cache": {
            "enabled": true,
            "ttl_seconds": 3600,
            "max_size_mb": 512
          },
          "api": {
            "host": "0.0.0.0",
            "port": 8080,
            "rate_limit": {
              "requests_per_minute": 120,
              "burst": 30
            },
            "timeouts": {
              "read_ms": 5000,
              "write_ms": 10000,
              "idle_ms": 60000
            }
          },
          "feature_flags": {
            "new_dashboard": true,
            "beta_analytics": false,
            "improved_search": true
          }
        }
          "#,
        ),
        (
            "Real world json 4",
            r#"{
          "app_name": "DataProcessor",
          "version": "2.1.3",
          "environment": "production",
          "debug": false,
          "log_level": "info",
          "database": {
            "main": {
              "host": "db-primary.internal",
              "port": 5432,
              "name": "app_production",
              "user": "app_user",
              "max_connections": 50,
              "timeout_ms": 5000
            },
            "replica": {
              "host": "db-replica.internal",
              "port": 5432,
              "name": "app_production_replica",
              "user": "app_readonly",
              "max_connections": 25,
              "timeout_ms": 3000
            }
          },
          "cache": {
            "enabled": true,
            "ttl_seconds": 3600,
            "max_size_mb": 512
          },
          "api": {
            "host": "0.0.0.0",
            "port": 8080,
            "rate_limit": {
              "requests_per_minute": 120,
              "burst": 30
            },
            "timeouts": {
              "read_ms": 5000,
              "write_ms": 10000,
              "idle_ms": 60000
            }
          },
          "feature_flags": {
            "new_dashboard": true,
            "beta_analytics": false,
            "improved_search": true
          }
        }"#,
        ),
        (
            "Real world json 5",
            r#"
          {
            "app_name": "DataProcessor",
            "version": "2.1.3",
            "environment": "production",
            "debug": false,
            "log_level": "info",
            "database": {
              "main": {
                "host": "db-primary.internal",
                "port": 5432,
                "name": "app_production",
                "user": "app_user",
                "max_connections": 50,
                "timeout_ms": 5000
              },
              "replica": {
                "host": "db-replica.internal",
                "port": 5432,
                "name": "app_production_replica",
                "user": "app_readonly",
                "max_connections": 25,
                "timeout_ms": 3000
              }
            },
            "cache": {
              "enabled": true,
              "ttl_seconds": 3600,
              "max_size_mb": 512
            },
            "api": {
              "host": "0.0.0.0",
              "port": 8080,
              "rate_limit": {
                "requests_per_minute": 120,
                "burst": 30
              },
              "timeouts": {
                "read_ms": 5000,
                "write_ms": 10000,
                "idle_ms": 60000
              }
            },
            "feature_flags": {
              "new_dashboard": true,
              "beta_analytics": false,
              "improved_search": true
            }
          }"#,
        ),
        (
            "Real world json 6",
            r#"
          {
            "event_id": "evt_0ab1cde23f4g5h6i",
            "event_type": "page_view",
            "timestamp": "2024-03-12T08:14:27.345Z",
            "user": {
              "id": "u_789012",
              "anonymous_id": "anon_6c7d8e9f0a",
              "device_id": "dev_3e4f5g6h7i",
              "session_id": "sess_1b2c3d4e5f"
            },
            "context": {
              "page": {
                "url": "https://example.com/products/smart-home",
                "title": "Smart Home Products | Example Store",
                "referrer": "https://google.com",
                "path": "/products/smart-home"
              },
              "device": {
                "type": "desktop",
                "manufacturer": "Apple",
                "model": "MacBook Pro",
                "screen": {
                  "width": 1440,
                  "height": 900
                }
              },
              "browser": {
                "name": "Chrome",
                "version": "99.0.4844.51"
              },
              "os": {
                "name": "macOS",
                "version": "12.3"
              },
              "location": {
                "country": "US",
                "region": "CA",
                "city": "San Francisco",
                "timezone": "America/Los_Angeles"
              }
            },
            "properties": {
              "duration_ms": 5327,
              "is_logged_in": true,
              "tags": ["homepage", "featured", "promo"],
              "utm": {
                "source": "newsletter",
                "medium": "email",
                "campaign": "spring_sale_2024"
              }
            }
          }
          "#,
        ),
        (
            "Deeply nested",
            r#"{
          "config": {
            "level1": {
              "level2": {
                "level3": {
                  "level4": {
                    "level5": {
                      "level6": {
                        "level7": {
                          "value": "deeply nested value",
                          "enabled": true,
                          "numbers": [1, 2, 3, 4, 5],
                          "settings": {
                            "mode": "advanced",
                            "retries": 3
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }"#,
        ),
        (
            "Array heavy",
            r#"{
          "feed": {
            "user_id": "u_12345",
            "posts": [
              {
                "id": "post_001",
                "author": "user_789",
                "content": "Just launched our new product! Check it out at example.com/new",
                "timestamp": "2024-03-13T14:27:32Z",
                "likes": 24,
                "comments": [
                  {
                    "id": "comment_001",
                    "author": "user_456",
                    "content": "Looks amazing! Cant wait to try it.",
                    "timestamp": "2024-03-13T14:35:12Z",
                    "likes": 3
                  },
                  {
                    "id": "comment_002",
                    "author": "user_789",
                    "content": "Thanks! Let me know what you think after youve tried it.",
                    "timestamp": "2024-03-13T14:42:45Z",
                    "likes": 1
                  }
                ],
                "tags": ["product", "launch", "technology"]
              },
              {
                "id": "post_002",
                "author": "user_123",
                "content": "Beautiful day for hiking! #nature #outdoors",
                "timestamp": "2024-03-13T11:15:22Z",
                "likes": 57,
                "comments": [
                  {
                    "id": "comment_003",
                    "author": "user_345",
                    "content": "Where is this? So beautiful!",
                    "timestamp": "2024-03-13T11:22:05Z",
                    "likes": 2
                  },
                  {
                    "id": "comment_004",
                    "author": "user_123",
                    "content": "Mount Rainier National Park!",
                    "timestamp": "2024-03-13T11:30:16Z",
                    "likes": 3
                  }
                ],
                "tags": ["nature", "outdoors", "hiking"],
                "location": {
                  "name": "Mount Rainier National Park",
                  "latitude": 46.8800,
                  "longitude": -121.7269
                }
              }
            ],
            "has_more": true,
            "next_cursor": "cursor_xyz123"
          }
        }"#,
        ),
    ];

    for (size_name, json_payload) in json_sizes.iter() {
        let query = format!("SELECT jsonb('{}')", json_payload.replace("'", "\\'"));

        let mut group = criterion.benchmark_group(format!("JSONB Size - {size_name}"));

        group.bench_function("Limbo", |b| {
            let mut stmt = limbo_conn.prepare(&query).unwrap();
            b.iter(|| {
                loop {
                    match stmt.step().unwrap() {
                        turso_core::StepResult::Row => {}
                        turso_core::StepResult::IO => {
                            stmt.run_once().unwrap();
                        }
                        turso_core::StepResult::Done => {
                            break;
                        }
                        turso_core::StepResult::Interrupt | turso_core::StepResult::Busy => {
                            unreachable!();
                        }
                    }
                }
                stmt.reset();
            });
        });

        if enable_rusqlite {
            let sqlite_conn = rusqlite_open();

            group.bench_function("Sqlite3", |b| {
                let mut stmt = sqlite_conn.prepare(&query).unwrap();
                b.iter(|| {
                    let mut rows = stmt.raw_query();
                    while let Some(row) = rows.next().unwrap() {
                        black_box(row);
                    }
                });
            });
        }

        group.finish();
    }
}

fn bench_sequential_jsonb(criterion: &mut Criterion) {
    // Flag to disable rusqlite benchmarks if needed
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false, false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Select a subset of JSON payloads to use in the sequential test
    let json_payloads = [
        ("Small", r#"{"id": 1, "name": "Test"}"#),
        (
            "Medium",
            r#"{"id": 1, "name": "Test", "attributes": {"color": "blue", "size": "medium", "tags": ["tag1", "tag2", "tag3"]}}"#,
        ),
        (
            "Real world json #1",
            r#"{
          "user": {
            "id": "usr_7f8d3a2e",
            "name": "Jane Smith",
            "email": "jane.smith@example.com",
            "verified": true,
            "created_at": "2023-05-12T15:42:31Z",
            "preferences": {
              "theme": "dark",
              "notifications": {
                "email": true,
                "push": false,
                "sms": true
              },
              "language": "en-US"
            }
          }
        }"#,
        ),
        (
            "Real world json #2",
            r#"{
      "feed": {
        "user_id": "u_12345",
        "posts": [
          {
            "id": "post_001",
            "author": "user_789",
            "content": "Just launched our new product! Check it out at example.com/new",
            "timestamp": "2024-03-13T14:27:32Z",
            "likes": 24,
            "comments": [
              {
                "id": "comment_001",
                "author": "user_456",
                "content": "Looks amazing! Cant wait to try it.",
                "timestamp": "2024-03-13T14:35:12Z",
                "likes": 3
              },
              {
                "id": "comment_002",
                "author": "user_789",
                "content": "Thanks! Let me know what you think after youve tried it.",
                "timestamp": "2024-03-13T14:42:45Z",
                "likes": 1
              }
            ],
            "tags": ["product", "launch", "technology"]
          },
          {
            "id": "post_002",
            "author": "user_123",
            "content": "Beautiful day for hiking! #nature #outdoors",
            "timestamp": "2024-03-13T11:15:22Z",
            "likes": 57,
            "comments": [
              {
                "id": "comment_003",
                "author": "user_345",
                "content": "Where is this? So beautiful!",
                "timestamp": "2024-03-13T11:22:05Z",
                "likes": 2
              },
              {
                "id": "comment_004",
                "author": "user_123",
                "content": "Mount Rainier National Park!",
                "timestamp": "2024-03-13T11:30:16Z",
                "likes": 3
              }
            ],
            "tags": ["nature", "outdoors", "hiking"],
            "location": {
              "name": "Mount Rainier National Park",
              "latitude": 46.8800,
              "longitude": -121.7269
            }
          }
        ],
        "has_more": true,
        "next_cursor": "cursor_xyz123"
      }
    }"#,
        ),
    ];

    // Create a query that calls jsonb() multiple times in sequence
    let query = format!(
        "SELECT jsonb('{}'), jsonb('{}'), jsonb('{}'), jsonb('{}'), jsonb('{}'), jsonb('{}'), jsonb('{}'), jsonb('{}')",
        json_payloads[0].1.replace("'", "\\'"),
        json_payloads[1].1.replace("'", "\\'"),
        json_payloads[2].1.replace("'", "\\'"),
        json_payloads[3].1.replace("'", "\\'"),
        json_payloads[0].1.replace("'", "\\'"),
        json_payloads[1].1.replace("'", "\\'"),
        json_payloads[2].1.replace("'", "\\'"),
        json_payloads[3].1.replace("'", "\\'"),
    );

    let mut group = criterion.benchmark_group("Sequential JSONB Calls");

    group.bench_function("Limbo - Sequential", |b| {
        let mut stmt = limbo_conn.prepare(&query).unwrap();
        b.iter(|| {
            loop {
                match stmt.step().unwrap() {
                    turso_core::StepResult::Row => {}
                    turso_core::StepResult::IO => {
                        stmt.run_once().unwrap();
                    }
                    turso_core::StepResult::Done => {
                        break;
                    }
                    turso_core::StepResult::Interrupt | turso_core::StepResult::Busy => {
                        unreachable!();
                    }
                }
            }
            stmt.reset();
        });
    });

    if enable_rusqlite {
        let sqlite_conn = rusqlite_open();

        group.bench_function("Sqlite3 - Sequential", |b| {
            let mut stmt = sqlite_conn.prepare(&query).unwrap();
            b.iter(|| {
                let mut rows = stmt.raw_query();
                while let Some(row) = rows.next().unwrap() {
                    black_box(row);
                }
            });
        });
    }

    group.finish();
}

fn bench_json_patch(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false, false).unwrap();
    let limbo_conn = db.connect().unwrap();

    let json_patch_cases = [
        (
            "Simple Property Update",
            r#"{"name": "Original", "value": 42}"#,
            r#"{"name": "Updated", "value": 100}"#,
        ),
        (
            "Add New Property",
            r#"{"name": "Original", "value": 42}"#,
            r#"{"name": "Original", "value": 42, "description": "Added field"}"#,
        ),
        (
            "Remove Property",
            r#"{"name": "Original", "value": 42, "toRemove": true}"#,
            r#"{"name": "Original", "value": 42}"#,
        ),
        (
            "Nested Property Update",
            r#"{"name": "Original", "value": 42, "nested": {"a": 1, "b": 2}}"#,
            r#"{"name": "Updated", "value": 42, "nested": {"a": 10, "b": 2, "c": 3}}"#,
        ),
        (
            "Array Update",
            r#"{"items": ["apple", "banana", "cherry"]}"#,
            r#"{"items": ["avocado", "cherry", "dragon fruit"]}"#,
        ),
        (
            "Complex User Object Update",
            r#"{
                "user": {
                    "id": "usr_7f8d3a2e",
                    "name": "Jane Smith",
                    "email": "jane.smith@example.com",
                    "verified": true,
                    "preferences": {
                        "theme": "dark",
                        "notifications": {
                            "email": true,
                            "push": false,
                            "sms": true
                        },
                        "language": "en-US"
                    }
                }
            }"#,
            r#"{
                "user": {
                    "id": "usr_7f8d3a2e",
                    "name": "Jane Doe",
                    "email": "jane.doe@example.com",
                    "verified": true,
                    "preferences": {
                        "theme": "light",
                        "notifications": {
                            "email": true,
                            "push": true,
                            "sms": false
                        },
                        "language": "en-US"
                    },
                    "subscription": {
                        "plan": "premium",
                        "status": "active"
                    }
                }
            }"#,
        ),
        (
            "Large Config Update",
            r#"{
                "app_name": "DataProcessor",
                "version": "2.1.3",
                "environment": "production",
                "debug": false,
                "log_level": "info",
                "database": {
                    "main": {
                        "host": "db-primary.internal",
                        "port": 5432,
                        "name": "app_production",
                        "user": "app_user",
                        "max_connections": 50,
                        "timeout_ms": 5000
                    },
                    "replica": {
                        "host": "db-replica.internal",
                        "port": 5432,
                        "name": "app_production_replica",
                        "user": "app_readonly",
                        "max_connections": 25,
                        "timeout_ms": 3000
                    }
                },
                "cache": {
                    "enabled": true,
                    "ttl_seconds": 3600,
                    "max_size_mb": 512
                },
                "api": {
                    "host": "0.0.0.0",
                    "port": 8080,
                    "rate_limit": {
                        "requests_per_minute": 120,
                        "burst": 30
                    },
                    "timeouts": {
                        "read_ms": 5000,
                        "write_ms": 10000,
                        "idle_ms": 60000
                    }
                },
                "feature_flags": {
                    "new_dashboard": true,
                    "beta_analytics": false,
                    "improved_search": true
                }
            }"#,
            r#"{
                "app_name": "DataProcessor",
                "version": "2.2.0",
                "environment": "production",
                "debug": true,
                "log_level": "debug",
                "database": {
                    "main": {
                        "host": "db-primary.internal",
                        "port": 5432,
                        "name": "app_production",
                        "user": "app_user",
                        "max_connections": 100,
                        "timeout_ms": 5000
                    },
                    "replica": {
                        "host": "db-replica.internal",
                        "port": 5432,
                        "name": "app_production_replica",
                        "user": "app_readonly",
                        "max_connections": 25,
                        "timeout_ms": 3000
                    },
                    "backup": {
                        "host": "db-backup.internal",
                        "port": 5432
                    }
                },
                "cache": {
                    "enabled": true,
                    "ttl_seconds": 3600,
                    "max_size_mb": 1024
                },
                "api": {
                    "host": "0.0.0.0",
                    "port": 8080,
                    "rate_limit": {
                        "requests_per_minute": 240,
                        "burst": 30
                    },
                    "timeouts": {
                        "read_ms": 5000,
                        "write_ms": 10000,
                        "idle_ms": 60000
                    }
                },
                "feature_flags": {
                    "new_dashboard": true,
                    "beta_analytics": true,
                    "improved_search": true,
                    "ai_recommendations": true
                }
            }"#,
        ),
        (
            "Deeply Nested Social Feed Update",
            r#"{
                "feed": {
                    "user_id": "u_12345",
                    "posts": [
                        {
                            "id": "post_001",
                            "author": "user_789",
                            "content": "Just launched our new product!",
                            "likes": 24,
                            "comments": [
                                {
                                    "id": "comment_001",
                                    "author": "user_456",
                                    "content": "Looks amazing!",
                                    "likes": 3
                                },
                                {
                                    "id": "comment_002",
                                    "author": "user_789",
                                    "content": "Thanks!",
                                    "likes": 1
                                }
                            ]
                        }
                    ]
                }
            }"#,
            r#"{
                "feed": {
                    "user_id": "u_12345",
                    "posts": [
                        {
                            "id": "post_001",
                            "author": "user_789",
                            "content": "Updated product announcement!",
                            "likes": 35,
                            "comments": [
                                {
                                    "id": "comment_001",
                                    "author": "user_456",
                                    "content": "This is incredible!",
                                    "likes": 5
                                },
                                {
                                    "id": "comment_002",
                                    "author": "user_789",
                                    "content": "Thanks!",
                                    "likes": 1
                                },
                                {
                                    "id": "comment_003",
                                    "author": "user_555",
                                    "content": "Just ordered one!",
                                    "likes": 0
                                }
                            ],
                            "tags": ["product", "launch"]
                        }
                    ]
                }
            }"#,
        ),
    ];

    for (case_name, target_json, patch_json) in json_patch_cases.iter() {
        let query = format!(
            "SELECT json_patch('{}', '{}')",
            target_json.replace("'", "''"),
            patch_json.replace("'", "''")
        );

        let mut group = criterion.benchmark_group(format!("JSON Patch - {case_name}"));

        group.bench_function("Limbo", |b| {
            let mut stmt = limbo_conn.prepare(&query).unwrap();
            b.iter(|| {
                loop {
                    match stmt.step().unwrap() {
                        turso_core::StepResult::Row => {}
                        turso_core::StepResult::IO => {
                            stmt.run_once().unwrap();
                        }
                        turso_core::StepResult::Done => {
                            break;
                        }
                        turso_core::StepResult::Interrupt | turso_core::StepResult::Busy => {
                            unreachable!();
                        }
                    }
                }
                stmt.reset();
            });
        });

        if enable_rusqlite {
            let sqlite_conn = rusqlite_open();

            group.bench_function("Sqlite3", |b| {
                let mut stmt = sqlite_conn.prepare(&query).unwrap();
                b.iter(|| {
                    let mut rows = stmt.raw_query();
                    while let Some(row) = rows.next().unwrap() {
                        black_box(row);
                    }
                });
            });
        }

        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(Some(Options::default()))));
    targets = bench, bench_sequential_jsonb, bench_json_patch
}

criterion_main!(benches);
