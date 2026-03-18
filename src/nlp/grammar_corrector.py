"""Grammar corrector"""
from spellchecker import SpellChecker
import re
from src.monitoring.logger import logger

class GrammarCorrector:
    def __init__(self):
        self.spell = SpellChecker()
        self.spell.word_frequency.load_words(['sum', 'average', 'count', 'group', 'total'])
    
    def correct(self, query: str):
        words = query.split()
        corrected_words = []
        corrections = []
        
        for word in words:
            if re.search(r'[0-9]', word) or len(word) < 2:
                corrected_words.append(word)
                continue
            
            clean = re.sub(r'[^a-zA-Z]', '', word).lower()
            if clean and clean not in self.spell:
                corrected = self.spell.correction(clean)
                if corrected and corrected != clean:
                    corrected_words.append(corrected)
                    corrections.append({'original': clean, 'corrected': corrected})
                else:
                    corrected_words.append(word)
            else:
                corrected_words.append(word)
        
        corrected_query = ' '.join(corrected_words)
        if corrections:
            logger.info(f"Applied {len(corrections)} corrections")
        return corrected_query, corrections
