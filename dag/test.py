import hashlib
import base64
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

def encrypt(plaintext, key):
    iv =key.encode()
    key = hashlib.sha256(key.encode()).digest()
    cipher = Cipher(algorithms.AES(key), modes.CFB(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(plaintext.encode()) + encryptor.finalize()
    return base64.b64encode(iv + ciphertext).decode('utf-8')

def decrypt(ciphertext, key):
    key = hashlib.sha256(key.encode()).digest()
    ciphertext = base64.b64decode(ciphertext.encode())
    iv = ciphertext[:16]
    ciphertext = ciphertext[16:]
    cipher = Cipher(algorithms.AES(key), modes.CFB(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    plaintext = decryptor.update(ciphertext) + decryptor.finalize()
    return plaintext.decode()

def main():
    key = "cPwz5VBr7ugGAv15"
    print("Salt: ", key)
    # ciphertext = encrypt(plaintext, key)
    # print("Ciphertext: ", ciphertext)
    ciphertext = "Y1B3ejVWQnI3dWdHQXYxNQvqaF2UDrS4"
    result = decrypt(ciphertext, key)
    print("Plaintext: ", result)

if __name__ == '__main__':
    main()
