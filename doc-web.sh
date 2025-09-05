#!/bin/sh

port=9038
addr=127.0.0.1
docd=./

miniserve \
	--port ${port} \
	--interfaces "${addr}" \
	"${docd}"
