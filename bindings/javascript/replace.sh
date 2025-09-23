sed -i "s/$NAME_FROM/$NAME_TO/g" packages/common/package.json
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/native/package.json
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/browser/package.json
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/browser-common/package.json
sed -i "s/$NAME_FROM/$NAME_TO/g" sync/packages/common/package.json
sed -i "s/$NAME_FROM/$NAME_TO/g" sync/packages/native/package.json
sed -i "s/$NAME_FROM/$NAME_TO/g" sync/packages/browser/package.json

sed -i "s/$VERSION_FROM/$VERSION_TO/g" packages/common/package.json
sed -i "s/$VERSION_FROM/$VERSION_TO/g" packages/native/package.json
sed -i "s/$VERSION_FROM/$VERSION_TO/g" packages/browser/package.json
sed -i "s/$VERSION_FROM/$VERSION_TO/g" packages/browser-common/package.json
sed -i "s/$VERSION_FROM/$VERSION_TO/g" sync/packages/common/package.json
sed -i "s/$VERSION_FROM/$VERSION_TO/g" sync/packages/native/package.json
sed -i "s/$VERSION_FROM/$VERSION_TO/g" sync/packages/browser/package.json

sed -i "s/$NAME_FROM/$NAME_TO/g" packages/native/promise.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/native/compat.ts

sed -i "s/$NAME_FROM/$NAME_TO/g" packages/browser-common/index.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/browser/promise.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/browser/promise-bundle.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/browser/promise-default.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/browser/promise-vite-dev-hack.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/browser/promise-turbopack-hack.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/browser/index-default.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/browser/index-bundle.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/browser/index-vite-dev-hack.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/browser/index-turbopack-hack.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/browser/worker.ts

sed -i "s/$NAME_FROM/$NAME_TO/g" sync/packages/native/promise.ts

sed -i "s/$NAME_FROM/$NAME_TO/g" sync/packages/browser/promise.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" sync/packages/browser/promise-bundle.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" sync/packages/browser/promise-default.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" sync/packages/browser/promise-vite-dev-hack.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" sync/packages/browser/promise-turbopack-hack.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" sync/packages/browser/index-default.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" sync/packages/browser/index-bundle.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" sync/packages/browser/index-vite-dev-hack.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" sync/packages/browser/index-turbopack-hack.ts
sed -i "s/$NAME_FROM/$NAME_TO/g" sync/packages/browser/worker.ts
