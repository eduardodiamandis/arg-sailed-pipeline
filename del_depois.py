from dotenv import load_dotenv
import os

load_dotenv()
print(f"URL encontrada: {os.getenv('URL_SAILED')}")