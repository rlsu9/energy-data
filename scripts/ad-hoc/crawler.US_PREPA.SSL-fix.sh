#!/bin/zsh

# This fix is needed because the data source of US_PREPA, aka https://aeepr.com (as defined in US_PREPA.py)
#   is misconfigured and does not provide the intermediate CA certificate.

cwd=$(pwd)

set -e
# set -x

# Workaround: https://stackoverflow.com/questions/27068163/python-requests-not-handling-missing-intermediate-certificate-only-from-one-mach

conda install -c anaconda certifi   # or use pip
# "DigiCert TLS RSA SHA256 2020 CA1" is the name of the missing intermediate CA as of 4/29/2022
# The URL is from https://www.digicert.com/kb/digicert-root-certificates.htm
tmpdir=$(mktemp -d)
cd "$tmpdir"
missing_ca_file_url="https://cacerts.digicert.com/DigiCertTLSRSASHA2562020CA1-1.crt.pem"
missing_ca_file_name="${missing_ca_file_url##*/}"
wget "$missing_ca_file_url" -O "$missing_ca_file_name"

new_content="# Intermediate certificate \"DigiCert TLS RSA SHA256 2020 CA1\"
# Downloaded from $missing_ca_file_url
"
new_content+="$(cat "$missing_ca_file_name")"
cacert_file=$(python -c "import certifi; print(certifi.where())")
echo "New certificate to install to certifi CA cert file ($cacert_file):"
echo "$new_content"
read REPLY\?"Continue?"

echo "$new_content" >> $cacert_file
echo "Done"

cd "$cwd"

rm -rf "$tmpdir"
