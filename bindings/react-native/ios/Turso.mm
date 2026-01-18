#import "Turso.h"

#import <React/RCTBridge+Private.h>
#import <React/RCTUtils.h>
#import <ReactCommon/RCTTurboModule.h>
#import <jsi/jsi.h>

#import "TursoHostObject.h"

@implementation Turso

@synthesize bridge = _bridge;

RCT_EXPORT_MODULE()

+ (BOOL)requiresMainQueueSetup {
    return YES;
}

- (void)setBridge:(RCTBridge *)bridge {
    _bridge = bridge;

    RCTCxxBridge *cxxBridge = (RCTCxxBridge *)self.bridge;
    if (!cxxBridge.runtime) {
        return;
    }

    [self installLibrary];
}

- (void)installLibrary {
    RCTCxxBridge *cxxBridge = (RCTCxxBridge *)self.bridge;
    if (!cxxBridge.runtime) {
        return;
    }

    facebook::jsi::Runtime *runtime = (facebook::jsi::Runtime *)cxxBridge.runtime;
    if (!runtime) {
        return;
    }

    // Get the documents directory for database storage
    NSArray *paths = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES);
    NSString *documentsPath = [paths firstObject];

    // Check for app group (for sharing data between app and extensions)
    NSString *appGroup = [[NSBundle mainBundle] objectForInfoDictionaryKey:@"Turso_AppGroup"];
    if (appGroup) {
        NSURL *containerURL = [[NSFileManager defaultManager] containerURLForSecurityApplicationGroupIdentifier:appGroup];
        if (containerURL) {
            documentsPath = [containerURL path];
        }
    }

    // Get the call invoker
    std::shared_ptr<facebook::react::CallInvoker> callInvoker = cxxBridge.jsCallInvoker;

    // Install the Turso module
    turso::install(*runtime, callInvoker, [documentsPath UTF8String]);
}

// Synchronous method to check if the module is installed
RCT_EXPORT_BLOCKING_SYNCHRONOUS_METHOD(install) {
    [self installLibrary];
    return @YES;
}

@end
