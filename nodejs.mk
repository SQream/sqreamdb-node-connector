all: connectors-nodejs

connectors-nodejs:
    ifeq ($(wildcard node_modules/*),)
		-npm install
    endif
	-npm pack

clean:
	-rm -Rf node_modules package-lock.json
