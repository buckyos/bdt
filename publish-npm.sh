rm -rf ./tmp
mkdir -p ./tmp/publish

cp -r ./base	./tmp/publish/
cp -r ./bdt		./tmp/publish/
cp -r ./dht		./tmp/publish/
cp -r ./p2p		./tmp/publish/
cp -r ./sn		./tmp/publish/
cp -r ./tools	./tmp/publish/
cp ./index.js	./tmp/publish/
cp ./package.json ./tmp/publish/

cd tmp/publish

npm adduser
npm publish