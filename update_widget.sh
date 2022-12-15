#!/bin/bash

pushd viz-widget
git submodule update --remote --merge
git pull
npm install
npm run build
cp ./dist/viz-widget.html ../src/sql/index.html
popd


