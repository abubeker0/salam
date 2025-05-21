
BOT_TOKEN = "6057025819:AAFO9Rj6B84y-HF6er4LAwEEbdla45GE4Fg"  # Replace with your bot token
DB_NAME = "telegram_chat_bot"
DB_USER = "postgres"
DB_PASSWORD = "Emu091006"
DB_HOST = "localhost"
DB_PORT = "5432"
ADMIN_USER_ID=5935463391
 #Replace with your admin ID

#Construct the database URL.
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
TELEGRAM_PAYMENT_TOKEN = "your_telegram_payment_token"
CHAPA_SECRET_KEY = "CHASECK_TEST-1dOO7koopLCEZCvjR9vqy4fHtf2cLSwF"

#Optional: you can remove the individual database variables, if you want.
#DB_NAME = None, DB_USER = None, DB_PASSWORD = None, DB_HOST = None, DB_PORT = None
