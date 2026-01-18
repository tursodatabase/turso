#import <Foundation/Foundation.h>
#import <React/RCTBridgeModule.h>

@interface Turso : NSObject <RCTBridgeModule>

@property (nonatomic, assign) BOOL setBridgeOnMainQueue;

@end
