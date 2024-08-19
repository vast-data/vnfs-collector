#!/bin/bash

# Define file paths
CA_CERT="ca.crt"
CA_KEY="./truststore/ca.key"
CLIENT_DIR="client"
CLIENT_KEY="${CLIENT_DIR}/client.key"
CLIENT_CSR="${CLIENT_DIR}/client.csr"
CLIENT_CERT="${CLIENT_DIR}/client.crt"
PEM_FILE="${CLIENT_DIR}/client.pem"

VALIDITY_IN_DAYS=3650

# Create client directory if it does not exist
mkdir -p "${CLIENT_DIR}"

# Generate client private key (unencrypted)
echo "Generating client private key..."
openssl genpkey -algorithm RSA -out "${CLIENT_KEY}"
if [ $? -ne 0 ]; then
    echo "Failed to generate client private key."
    exit 1
fi

# Generate client certificate signing request (CSR)
echo "Generating client CSR..."
openssl req -new -key "${CLIENT_KEY}" -out "${CLIENT_CSR}"
if [ $? -ne 0 ]; then
    echo "Failed to generate client CSR."
    exit 1
fi

# Sign the client CSR with the CA certificate and key to create the client certificate
echo "Generating client certificate..."
openssl x509 -req -in "${CLIENT_CSR}" -CA "${CA_CERT}" -CAkey "${CA_KEY}" -CAcreateserial -out "${CLIENT_CERT}" -days "${VALIDITY_IN_DAYS}"
if [ $? -ne 0 ]; then
    echo "Failed to generate client certificate."
    exit 1
fi

# Optionally bundle the client key and certificate into a single PEM file
echo "Bundling client key and certificate into a single PEM file..."
cat "${CLIENT_KEY}" "${CLIENT_CERT}" > "${PEM_FILE}"
if [ $? -ne 0 ]; then
    echo "Failed to bundle client key and certificate."
    exit 1
fi

# Clean up CSR
echo "Cleaning up temporary files..."
rm -f "${CLIENT_CSR}"

echo "Client certificate and key generation complete. Files are stored in '${CLIENT_DIR}'."
