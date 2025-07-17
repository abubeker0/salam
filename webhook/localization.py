import gettext
import os
import logging

logger = logging.getLogger(__name__)

# --- Configuration for gettext ---
DOMAIN = 'messages'
LOCALE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'locale')

# Dictionary to store loaded translators, keyed by language code
loaded_translators = {}

def load_translator(lang_code: str):
    """
    Loads and returns a gettext.GNUTranslations object for the given language code.
    Handles FileNotFoundError and other exceptions during loading.
    """
    try:
        # gettext.translation will try to find the .mo file at LOCALE_DIR/<lang_code>/LC_MESSAGES/messages.mo
        translator = gettext.translation(DOMAIN, LOCALE_DIR, languages=[lang_code], fallback=True)
        logger.info(f"Localization: Successfully loaded translator for '{lang_code}'.")
        return translator
    except FileNotFoundError:
        logger.error(f"Localization: MO file for '{lang_code}' not found at expected path. "
                     f"Tried: {os.path.join(LOCALE_DIR, lang_code, 'LC_MESSAGES', DOMAIN + '.mo')}")
        # Fallback to English if the specific language MO file is missing
        return load_translator('en') # Recursive call, but should resolve to 'en'

    except Exception as e:
        logger.error(f"Localization: Error loading translator for '{lang_code}': {e}", exc_info=True)
        # Fallback to English for other loading errors
        return load_translator('en')

def setup_i18n():
    """
    Initializes and pre-loads translators for all supported languages.
    This should be called ONCE at your bot's startup.
    """
    global loaded_translators # Declare global to modify the dictionary
    supported_languages = ['en', 'am', 'or', 'ar'] # Define your supported languages explicitly

    for lang_code in supported_languages:
        # Load each translator and store it in the global dictionary
        # This approach ensures that even if loading fails, a fallback (usually English) is stored.
        loaded_translators[lang_code] = load_translator(lang_code)

    # Ensure English is always explicitly loaded and available as the ultimate fallback
    if 'en' not in loaded_translators or isinstance(loaded_translators['en'], gettext.NullTranslations):
        loaded_translators['en'] = load_translator('en') # Re-attempt to load 'en' if it failed or is null

    logger.info("Localization setup complete. Available translators: %s", list(loaded_translators.keys()))


def _(message_id: str, lang_code: str = 'en') -> str: # user_id parameter removed
    """
    The main localization function.
    It will attempt to translate `message_id` based on `lang_code`.
    """
    # If lang_code is not provided or is not in loaded_translators, default to 'en'
    translator = loaded_translators.get(lang_code)

    if not translator:
        # This should ideally not happen if setup_i18n is called correctly,
        # but provides a robust fallback.
        logger.warning(f"Localization: No translator found for '{lang_code}' for text '{message_id}'. Falling back to 'en'.")
        translator = loaded_translators.get('en')

    if translator:
        return translator.gettext(message_id)
    else:
        # Ultimate fallback if even the 'en' translator isn't available (critical error)
        logger.critical(f"Localization: No translator (not even 'en' fallback) found! Returning original text for '{message_id}'.")
        return message_id