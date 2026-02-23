import hashlib

def anonymize_user(user_email: str) -> str:
    """
    Hashes an email address to protect privacy (GDPR compliance).
    Example: 'mario.rossi@company.com' -> 'User_a1b2c3...'
    """
    if not user_email:
        return "Unknown_User"
    
    # Create a simple MD5 hash
    hash_object = hashlib.md5(user_email.encode())
    return f"User_{hash_object.hexdigest()[:8]}"