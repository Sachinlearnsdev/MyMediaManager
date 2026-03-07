#!/usr/bin/env python3
import re
from difflib import SequenceMatcher

class CentralLogic:
    # Known noise words stripped from filename before confidence scoring
    _NOISE_WORDS = frozenset([
        '480p','720p','1080p','2160p','4k',
        'x264','x265','h264','h265','hevc','av1',
        'bluray','webrip','webdl','hdrip','dvdrip','bdrip','brrip',
        'cam','ts','hdts','hdr','hdr10','10bit','8bit',
        'mkv','mp4','avi','mov','wmv','flv','webm','m4v',
        'remastered','korean',
        'aac','ac3','eac3','flac','mp3','remux','dts','truehd','atmos',
        'proper','repack','internal','multi',
    ])
    _YEAR_RE = re.compile(r'^(19|20)\d{2}$')

    def __init__(self):
        # Patterns for identifying the "Pivot Point"
        self.year_pattern = re.compile(r'\b(19|20)\d{2}\b')
        self.season_pattern = re.compile(r'\bS\d{2}E\d{2}\b', re.IGNORECASE)
        self.episode_pattern = re.compile(r'\b\d{1,3}\b') # For anime 01, 02

        # Matches CJK characters only (Kanji, Hiragana, Katakana, CJK symbols)
        # Preserves accented Latin characters (é, ü, ñ, etc.) for proper filenames
        self.non_latin_pattern = re.compile(
            r'[\u3000-\u303F\u3040-\u309F\u30A0-\u30FF'
            r'\u4E00-\u9FFF\uF900-\uFAFF\uFF00-\uFFEF]+'
        )

    def sanitize_for_filesystem(self, text):
        """
        Prevents the D061DC~9 bug by removing illegal characters 
        and Japanese text from the final filename/folder.
        """
        if not text: return "Unknown"
        
        # 1. Smart colon replacement
        # Between word chars (Re:ZERO) → dash without extra space
        text = re.sub(r'(\w):(\w)', r'\1-\2', text)
        # Colon followed by space or end (Title: Subtitle) → " - "
        text = re.sub(r':\s*', ' - ', text)
        
        # 2. Normalize " -Word" patterns to " - Word" (from API titles like "Re:ZERO -Starting...")
        text = re.sub(r' -(\w)', r' - \1', text)
        # Also normalize trailing "-" attached to words: "World-" -> "World"
        text = re.sub(r'(\w)-(\s|$)', r'\1\2', text)

        # 3. Kill the Kanji: Remove Japanese characters
        text = self.non_latin_pattern.sub('', text).strip()

        # 4. Illegal Character Sweep: Final protection for Linux/Windows
        text = re.sub(r'[\\/*?"<>|]', "", text)
        
        # 5. Clean up mess: Turn "Title  -  Sub" into "Title - Sub"
        text = re.sub(r'\s+', ' ', text).replace(' - -', ' -')
        
        # CRITICAL FIX: Strip trailing dashes and spaces.
        # "Season 02 - " becomes "Season 02"
        return text.strip(' -')

    def get_title_candidate(self, filename):
        """
        Slices the filename at the first sign of a Year or Season tag.
        Returns (Title Candidate, Year, Metadata).
        """
        # Clean extension
        clean_name = filename.rsplit('.', 1)[0]
        
        # 1. Look for SxxEyy (TV/Anime)
        tv_match = self.season_pattern.search(clean_name)
        if tv_match:
            pivot = tv_match.start()
            return clean_name[:pivot].strip().replace('.', ' '), None, clean_name[pivot:]

        # 2. Look for Year (Movies) - use LAST match to handle titles containing years
        # e.g. "1917 2019" -> candidate="1917", year="2019"
        # e.g. "Blade Runner 2049 2017" -> candidate="Blade Runner 2049", year="2017"
        year_matches = list(self.year_pattern.finditer(clean_name))
        if year_matches:
            last_match = year_matches[-1]
            pivot = last_match.start()
            year = last_match.group()
            return clean_name[:pivot].strip().replace('.', ' '), year, clean_name[pivot:]

        # 3. Fallback (If no pivot, return whole string)
        return clean_name.replace('.', ' '), None, ""

    def generate_query_matrix(self, title_candidate):
        """
        Generates the 5 sequential scenarios we discussed.
        No word swapping allowed.
        """
        words = title_candidate.split()
        n = len(words)
        if n == 0: return []

        matrix = []
        # A. Full String
        matrix.append(" ".join(words))

        # B. Right Strip (Contiguous)
        for i in range(n - 1, 0, -1):
            matrix.append(" ".join(words[:i]))

        # C. Left Strip
        for i in range(1, n):
            matrix.append(" ".join(words[i:]))

        # D. Outer Strip (Remove first and last)
        if n > 2:
            matrix.append(" ".join(words[1:-1]))

        # E. Center Extraction
        mid = n // 2
        if n >= 2:
            if n % 2 == 0: # Even
                matrix.append(" ".join(words[mid-1 : mid+1]))
            else: # Odd
                matrix.append(words[mid])

        # De-duplicate while keeping original order
        return list(dict.fromkeys(matrix))

    def calculate_confidence(self, filename, api_title, api_year=None, file_year=None,
                             year_mode="penalty"):
        """
        The Sequential Scorer (Bidirectional).
        Returns a raw 0-100 score based on Sequential Word Parity.

        Enhancements:
        - Strips known noise words from filename before scoring
        - Word-count disparity penalty (prevents "Friends" matching "One Week Friends")
        - Prefix match bonus (handles "Dr Strangelove" vs full long TMDB title)

        year_mode:
          "penalty" (default) — ±1 year = -30 penalty, ±2+ years = instant 0
          "tiebreaker" — year mismatch only used as tiebreaker (returns score + year_bonus);
                         ±2+ years = -10 penalty (instead of instant fail)
        """
        fn = self._normalize(filename)
        at = self._normalize(api_title)

        # Strip noise words from filename to prevent score dilution
        fn = ' '.join(w for w in fn.split() if w not in self._NOISE_WORDS)

        api_words = at.split()
        fn_words = fn.split()
        if not api_words or not fn_words: return 0

        # 1. Year Check
        year_penalty = 0
        if api_year and file_year:
            try:
                diff = abs(int(str(api_year)[:4]) - int(str(file_year)[:4]))
                if year_mode == "tiebreaker":
                    # Movies: year is disambiguation helper, not strict filter
                    # Exact match → +5 bonus, ±1 → no change, ±2+ → -10 small penalty
                    if diff == 0:
                        year_penalty = -5  # Bonus (negative penalty = boost)
                    elif diff >= 2:
                        year_penalty = 10
                else:
                    # Default: penalty mode (series, anime, TV)
                    if diff == 1:
                        year_penalty = 30  # ±1 year: penalty instead of instant fail
                    elif diff >= 2:
                        return 0  # ±2+ years: instant fail
            except (ValueError, TypeError):
                pass

        # 2. Forward: what fraction of API words appear in filename (sequentially)
        forward_score = self._sequential_match_score(fn, api_words)

        # 3. Reverse: what fraction of filename words appear in API title (sequentially)
        reverse_score = self._sequential_match_score(at, fn_words)

        # 4. Bonus for First-Word Anchor
        anchor_bonus = 10 if api_words and fn.startswith(api_words[0]) else 0

        # Take the better direction + anchor, capped at 100
        base = min(100, max(forward_score, reverse_score) + anchor_bonus)

        # 5. Identify significant (non-year) title words for adjustments
        fn_title = [w for w in fn_words if not self._YEAR_RE.match(w)]
        at_title = [w for w in api_words if not self._YEAR_RE.match(w)]

        # 6. Prefix match bonus: when filename (2+ title words) is a prefix of API title
        is_prefix = False
        if len(fn_title) >= 2 and len(at_title) > len(fn_title):
            is_prefix = all(
                i < len(at_title) and fn_title[i] == at_title[i]
                for i in range(len(fn_title))
            )

        # 7. Apply adjustments
        word_diff = len(at_title) - len(fn_title)
        if is_prefix:
            # Short form of a long title (e.g., "Dr Strangelove" -> full TMDB title)
            result = base + 15
        elif word_diff >= 1:
            # Penalize when API title has more words than filename
            # diff=1: -5 (catches "Friends" vs "Isshuukan Friends")
            # diff=2: -10, diff=3: -15, etc. capped at 25
            penalty = min(25, word_diff * 5)
            result = base - penalty
        elif word_diff <= -2:
            # Penalize when filename has many more words than API title
            # Prevents "House of Cards" from matching just "House" at 100%
            penalty = min(25, abs(word_diff) * 5)
            result = base - penalty
        else:
            result = base

        return min(100, max(0, result - year_penalty))

    def _sequential_match_score(self, haystack, needle_words):
        """Score: fraction of needle_words found as whole words sequentially in haystack."""
        hay_words = haystack.split()
        last_idx = -1
        matches = 0
        for word in needle_words:
            for i in range(last_idx + 1, len(hay_words)):
                if hay_words[i] == word:
                    matches += 1
                    last_idx = i
                    break
        return (matches / len(needle_words)) * 100 if needle_words else 0

    # Regex for non-Latin episode title detection (shared by series + anime processors)
    _NON_LATIN_EP_RE = re.compile(
        r'[\u0900-\u097F'    # Devanagari (Hindi)
        r'\u0980-\u09FF'     # Bengali
        r'\u0A00-\u0A7F'     # Gurmukhi (Punjabi)
        r'\u0B00-\u0B7F'     # Oriya
        r'\u0B80-\u0BFF'     # Tamil
        r'\u0C00-\u0C7F'     # Telugu
        r'\u0C80-\u0CFF'     # Kannada
        r'\u0D00-\u0D7F'     # Malayalam
        r'\u0E00-\u0E7F'     # Thai
        r'\u0600-\u06FF'     # Arabic
        r'\u0590-\u05FF'     # Hebrew
        r'\u3040-\u309F'     # Hiragana
        r'\u30A0-\u30FF'     # Katakana
        r'\u4E00-\u9FFF'     # CJK Unified
        r'\uAC00-\uD7AF'     # Korean Hangul
        r']'
    )

    @staticmethod
    def is_non_latin_title(text):
        """Check if text contains non-Latin script characters (Hindi, CJK, Arabic, etc.)."""
        return bool(CentralLogic._NON_LATIN_EP_RE.search(text)) if text else False

    @staticmethod
    def tvmaze_spinoff_fallback(results, query, pool, media_type='tv'):
        """Append TVMaze results when no TVDB result title exactly matches query.

        Prevents spinoff-only results (e.g., "Silicon Valley Hidden Stories"
        instead of "Silicon Valley").
        """
        q_norm = re.sub(r'[^a-z0-9 ]', ' ', query.lower()).strip()
        has_exact = False
        for r in results:
            t_norm = re.sub(r'[^a-z0-9 ]', ' ', (r.get('title') or '').lower()).strip()
            if t_norm == q_norm:
                has_exact = True
                break
            for alias in (r.get('aliases') or []):
                if re.sub(r'[^a-z0-9 ]', ' ', alias.lower()).strip() == q_norm:
                    has_exact = True
                    break
            if has_exact:
                break
        if not has_exact and results:
            tvmaze = pool.get('tvmaze')
            if tvmaze:
                try:
                    return results + tvmaze.search(query, media_type=media_type)
                except Exception:
                    pass
        return results

    def _normalize(self, text):
        import unicodedata
        # Remove Japanese characters first so they don't count as "missing words"
        text = self.non_latin_pattern.sub(' ', text)
        # Strip diacritics: é→e, ā→a, ñ→n, etc. (prevents "Amélie" ≠ "Amelie")
        text = unicodedata.normalize('NFD', text)
        text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
        # Standard lower-case alphanumeric clean
        return re.sub(r"[^a-z0-9 ]+", " ", text.lower()).strip()