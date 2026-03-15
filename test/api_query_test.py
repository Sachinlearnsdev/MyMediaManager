#!/usr/bin/env python3
"""
Better API Query Tester - For selecting best data source
"""

import requests
import json
import time

def test_jikan_detailed(show_name, season):
    """Detailed Jikan test for a specific show"""
    print(f"\n{'='*70}")
    print(f"JIKAN: {show_name} Season {season}")
    print('='*70)

    query = f"{show_name} Season {season}" if season > 1 else show_name

    try:
        time.sleep(1)
        res = requests.get("https://api.jikan.moe/v4/anime",
                          params={"q": query, "limit": 5})
        data = res.json().get('data', [])

        print(f"\nAvailable matches from Jikan:")
        for i, item in enumerate(data[:5], 1):
            mal_id = item['mal_id']
            title = item.get('title', 'N/A')
            title_en = item.get('title_english', 'N/A')
            airing_type = item.get('type', 'Unknown')
            status = item.get('status', 'Unknown')

            print(f"\n  [{i}] MAL ID: {mal_id}")
            print(f"      Title: {title}")
            print(f"      English: {title_en}")
            print(f"      Type: {airing_type} | Status: {status}")

            # Fetch episode data for this MAL ID
            if airing_type and airing_type.upper() != 'MOVIE':
                try:
                    time.sleep(0.5)
                    ep_res = requests.get(
                        f"https://api.jikan.moe/v4/anime/{mal_id}/episodes",
                        params={"page": 1}
                    )
                    eps = ep_res.json().get('data', [])
                    if eps:
                        print(f"      Episodes (first 3):")
                        for ep in eps[:3]:
                            ep_num = ep.get('mal_id')
                            ep_title_en = ep.get('title', 'N/A')
                            ep_title_jp = ep.get('title_romanji', '')
                            print(f"        Ep {ep_num}: {ep_title_en}")
                            if ep_title_jp and ep_title_jp != ep_title_en:
                                print(f"           (JP: {ep_title_jp})")
                except Exception as e:
                    print(f"      Episode fetch error: {type(e).__name__}")
    except Exception as e:
        print(f"Error: {e}")


def test_anilist_detailed(mal_id):
    """Detailed AniList test"""
    print(f"\n{'='*70}")
    print(f"ANILIST: MAL ID {mal_id}")
    print('='*70)

    query = '''
    query ($id: Int) {
      Media (idMal: $id, type: ANIME) {
        title {
          english
          romaji
        }
        episodes
        streamingEpisodes (limit: 5) {
          title
          site
        }
      }
    }
    '''

    try:
        res = requests.post("https://graphql.anilist.co",
                           json={'query': query, 'variables': {'id': mal_id}},
                           timeout=10)
        resp_data = res.json()

        if 'errors' in resp_data:
            print(f"  Query error: {resp_data['errors']}")
            return

        data = resp_data.get('data', {}).get('Media')
        if not data:
            print("  No data returned")
            return

        title = data.get('title', {})
        print(f"\n  English Title: {title.get('english', 'N/A')}")
        print(f"  Romaji Title: {title.get('romaji', 'N/A')}")
        print(f"  Total Episodes: {data.get('episodes', 'Unknown')}")

        streaming = data.get('streamingEpisodes', [])
        if streaming:
            print(f"\n  Streaming Episodes (first 5):")
            for ep in streaming[:5]:
                ep_title = ep.get('title', 'N/A')
                site = ep.get('site', 'Unknown')
                print(f"    - {ep_title} ({site})")
        else:
            print(f"\n  Streaming Episodes: None available")

    except Exception as e:
        print(f"  Error: {e}")


# Test cases
TESTS = [
    ("Haikyuu", 3),
    ("Jujutsu Kaisen", 2),
    ("One Piece", 1),
    ("Attack on Titan", 4),
]

if __name__ == "__main__":
    for show_name, season in TESTS:
        test_jikan_detailed(show_name, season)
        time.sleep(1)

        # Get MAL ID for AniList
        try:
            res = requests.get("https://api.jikan.moe/v4/anime",
                              params={"q": show_name, "limit": 1})
            mal_id = res.json().get('data', [{}])[0].get('mal_id')
            if mal_id:
                test_anilist_detailed(mal_id)
        except:
            pass

        time.sleep(1)

    print("\n\n" + "="*70)
    print("SUMMARY: Best API Selection")
    print("="*70)
    print("""
KEY FINDINGS:

1. **One Piece Issue**:
   - Jikan returns "One Piece Movie 01" (MAL 459) on first search
   - Solution: Filter by type != 'MOVIE' or get second result for TV series (MAL 21)

2. **Haikyuu/AoT Season Titles**:
   - Jikan provides correct English season-specific titles
   - E.g., "Haikyu!! 3rd Season" or "Attack on Titan: Final Season"
   - AniList should use these instead of trying to extract

3. **Episode Titles**:
   - Jikan's 'title' field has English episode names
   - AniList streamingEpisodes sometimes empty
   - Solution: Use Jikan as primary source, fallback to AniList

4. **Season Subtitle Logic**:
   - If season title contains generic phrase like "Season 2", "2nd Season", "3rd Season" -> strip it
   - If it has actual meaning like "Final Season" or "To the Top" -> keep it
   - For Haikyuu S3: Use full title "Karasuno High School vs Shiratorizawa Academy"

5. **TVDB/TMDB**:
   - Require API keys
   - Better for live-action TV, less reliable for anime
   - Skip for anime processing
    """)
