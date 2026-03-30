pack-native:
	npm publish --dry-run && npm pack
pack-wasm:
	cp package.json package.native.json
	cp package.browser.json package.json
	npm publish --dry-run && npm pack; cp package.native.json package.json

publish-native:
	npm publish --access public
publish-wasm:
	cp package.json package.native.json
	cp package.browser.json package.json
	npm publish --access public; cp package.native.json package.json

publish-native-next:
	npm publish --tag next --access public
publish-wasm-next:
	cp package.json package.native.json
	cp package.browser.json package.json
	npm publish --tag next --access public; cp package.native.json package.json

