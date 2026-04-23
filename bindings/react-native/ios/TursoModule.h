#import <Foundation/Foundation.h>
#import <React/RCTBridgeModule.h>
#import <React/RCTInvalidating.h>

@interface Turso : NSObject <RCTBridgeModule, RCTInvalidating>

@property (nonatomic, assign) BOOL setBridgeOnMainQueue;

@end
