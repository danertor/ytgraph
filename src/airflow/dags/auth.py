"""
Authentication file to access private data like Keys and passwords.
Usually a Key Vault service should be used to store private keys.
"""
import os


AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
