all: connectors-nodejs

connectors-nodejs: node_modules
	-npm pack

node_modules:
	-npm install

clean:
	-rm -Rf node_modules package-lock.json
