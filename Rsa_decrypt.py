from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization

# The encrypted message and private key
encrypted_message = b'\x82\xfb\x92\xb1\x98\xf1\xc7\xb2\x3c...\x46\xdc\x3d'
private_key_str = """-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA4v3
...L8Q9xNBVn9XJ5
-----END RSA PRIVATE KEY-----"""

# Load the private key from the string
private_key = serialization.load_pem_private_key(
    private_key_str.encode(),
    password=None,
    backend=default_backend()
)

# Decrypt the message
decrypted = private_key.decrypt(
    encrypted_message,
    padding.OAEP(
        mgf=padding.MGF1(algorithm=hashes.SHA256()),
        algorithm=hashes.SHA256(),
        label=None
    )
)

# Print the decrypted message
print(decrypted.decode())
