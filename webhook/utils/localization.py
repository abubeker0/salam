# utils/localization.py

import gettext
import os
import logging

logger = logging.getLogger(__name__)

# --- Configuration for gettext ---
DOMAIN = 'messages'
# Adjust LOCALE_DIR if your structure is different.
# It should point to the directory containing 'en', 'am', 'or', 'ar' folders.
LOCALE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'locale')

default_translator = gettext.NullTranslations()
loaded_translators = {}

def get_translator(lang_code: str):
    """
    Returns a gettext.GNUTranslations object for the given language code.
    Caches translators to avoid re-reading .mo files.
    """
    if lang_code not in loaded_translators:
        try:
            t = gettext.translation(DOMAIN, LOCALE_DIR, languages=[lang_code], fallback=True)
            loaded_translators[lang_code] = t
            logger.info(f"Loaded translator for language: {lang_code}")
        except Exception as e:
            logger.error(f"Could not load translator for {lang_code} from {LOCALE_DIR}/{lang_code}: {e}")
            loaded_translators[lang_code] = default_translator
    return loaded_translators[lang_code]

def _(message_id: str, user_id: int = None, lang_code: str = None) -> str:
    """
    The main localization function.
    It will attempt to translate `message_id` based on `lang_code`.
    """
    # In this setup, _() relies on lang_code being passed or defaulted to 'en'.
    # It does NOT fetch from DB itself, as that's now in handlers.py.
    translator = get_translator(lang_code) if lang_code else default_translator
    return translator.gettext(message_id)