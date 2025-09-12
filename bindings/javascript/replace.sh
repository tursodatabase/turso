sed -i "s/$NAME_FROM/$NAME_TO/g" packages/common/package.json
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/native/package.json
sed -i "s/$NAME_FROM/$NAME_TO/g" packages/browser/package.json

sed -i "s/$VERSION_FROM/$VERSION_TO/g" packages/common/package.json
sed -i "s/$VERSION_FROM/$VERSION_TO/g" packages/native/package.json
sed -i "s/$VERSION_FROM/$VERSION_TO/g" packages/browser/package.json

sed -i "s/$NAME_FROM\/database-common/$NAME_TO\/database-common/g" packages/native/promise.ts
sed -i "s/$NAME_FROM\/database-common/$NAME_TO\/database-common/g" packages/native/compat.ts
sed -i "s/$NAME_FROM\/database-common/$NAME_TO\/database-common/g" packages/browser/promise.ts
