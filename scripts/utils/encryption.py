# encryption.py - Шифрование данных
from cryptography.fernet import Fernet
import hashlib


class Encryptor:
    def __init__(self, key):
        """Инициализация с ключом из .env"""
        self.cipher = Fernet(key.encode())

    def encrypt(self, text):
        """Шифрует текст"""
        if not text:
            return ""
        return self.cipher.encrypt(text.encode()).decode()

    def decrypt(self, encrypted_text):
        """Расшифровывает текст (если нужно)"""
        if not encrypted_text:
            return ""
        return self.cipher.decrypt(encrypted_text.encode()).decode()

    @staticmethod
    def hash_text(text):
        """Создает SHA-256 хеш для поиска"""
        if not text:
            return ""
        return hashlib.sha256(text.encode()).hexdigest()