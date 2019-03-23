pip2 install gsutil
mkdir -p data
gsutil -m cp -r gs://charlottesville-council-minutes ./data
cp -r data ../data