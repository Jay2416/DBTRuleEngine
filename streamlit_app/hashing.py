import hashlib

# The passwords we want to hash
passwords = {
    "chagan_t": "man1", 
    "magan_s": "man2", 
    "chotu_s": "man3"
}

for username, pwd in passwords.items():
    # Calculate the correct SHA-256 hash
    real_hash = hashlib.sha256(pwd.encode()).hexdigest()

    print(real_hash)

print(passwords)