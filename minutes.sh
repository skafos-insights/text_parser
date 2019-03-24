pip2 install gsutil
mkdir -p data
gsutil -m cp -r gs://charlottesville-council-minutes ./
cp -r charlottesville-council-minutes/data ../data
