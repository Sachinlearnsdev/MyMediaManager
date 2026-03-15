#!/usr/bin/env python3
"""
Real-World Stress Test Generator
Creates exact replicas of common torrent/download folder structures.
Based on actual media collections with messy filenames, nested folders, and junk.

Usage: python test/create_real_world_test.py [--clean]
  --clean  Remove existing Drop folders before creating test files
"""

import os
import sys
import shutil
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "bin"))

import common
_, CFG = common.setup_logger("test_setup")

DATA_ROOT = Path(CFG['paths']['roots']['data'])
DROP_MOVIES = DATA_ROOT / CFG['paths']['movie_pipeline']['input_drop']
DROP_SHOWS = DATA_ROOT / CFG['paths']['series_pipeline']['input_drop']

DUMMY_CONTENT = b'\x1a\x45\xdf\xa3' + b'\x00' * 1020  # 1KB with MKV magic bytes


def mkv(path: Path):
    """Create a dummy video file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(DUMMY_CONTENT)

def junk(path: Path):
    """Create a junk/non-video file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("placeholder", encoding='utf-8')


def create_movies():
    """Create all movie test files in Drop_Movies."""
    D = DROP_MOVIES
    count = 0

    # ========================
    # STANDALONE MOVIE FILES
    # ========================

    # Hollywood - Standard scene releases
    for f in [
        "[YTS.MX] Inception.2010.1080p.BluRay.x264.AAC-[YTS.MX].mkv",
        "Dune.Part.Two.2024.IMAX.2160p.WEB-DL.DDP5.1.Atmos.DV.HDR.H.265-FLUX.mkv",
    ]:
        mkv(D / f); count += 1

    # Bollywood / Tollywood / International
    for f in [
        "3.Idiots.2009.Hindi.1080p.BluRay.x264.DTS-HD.MA.5.1-Hon3y.mkv",
        "Baahubali.2.The.Conclusion.2017.Telugu.4K.UHD.WEB-DL.x265-HEVC.mkv",
        "Crouching.Tiger.Hidden.Dragon.2000.REMASTERED.1080p.BluRay.x264-DEPTH.mkv",
        "Parasite.2019.KOREAN.1080p.BluRay.H264.AAC-VXT.mkv",
    ]:
        mkv(D / f); count += 1

    # Anime Movies - Brackets & group names
    for f in [
        "[HorribleSubs] Your Name (2016) [1080p].mkv",
        "[Breeze] Haikyuu!! Movie - Owari to Hajimari (2015) [1080p].mkv",
        "[Breeze] Haikyuu!! Movie - Shousha to Haisha (2015) [1080p].mkv",
        "[ReleaseGroup] Haikyuu!! The Dumpster Battle (2024) 1080p.mkv",
        "[DB]Haikyuu!! Movie Gomisuteba no Kessen_-_(Dual Audio_10bit_BD1080p_x265).mkv",
    ]:
        mkv(D / f); count += 1

    # Edge case - No year, no useful info
    mkv(D / "[Unknown-Group] Super.Cool.Video.File.With.No.Info.1080p.mkv"); count += 1

    # ========================
    # MCU FRANCHISE (YIFY/YTS nested folders with junk)
    # ========================

    # Captain America: The First Avenger (2011)
    ca1 = D / "Captain America - The First Avenger (2011)"
    mkv(ca1 / "Captain.America.The.First.Avenger.1080p.BrRip.x264.YIFY.mp4"); count += 1
    junk(ca1 / "Captain.America.The.First.Avenger.1080p.BrRip.x264.YIFY.srt")
    junk(ca1 / "WWW.YIFY-TORRENTS.COM.jpg")
    junk(ca1 / "Other" / "AhaShare.com.txt")
    junk(ca1 / "Other" / "Torrent downloaded from Demonoid.com - Copy.txt")

    # Captain America: The Winter Soldier (2014)
    ca2 = D / "Captain America The Winter Soldier (2014) [1080p]"
    mkv(ca2 / "Captain.America.The.Winter.Soldier.2014.1080p.BluRay.x264.YIFY.mp4"); count += 1
    junk(ca2 / "WWW.YTS.RE.jpg")

    # Captain America: Civil War (2016)
    ca3 = D / "Captain America Civil War (2016) [1080p]"
    mkv(ca3 / "Captain.America.Civil.War.2016.1080p.BluRay.x264-[YTS.AG].mp4"); count += 1
    junk(ca3 / "WWW.YTS.AG.jpg")

    # Iron Man (2008)
    im1 = D / "Iron Man [1080p]"
    mkv(im1 / "Iron.Man.2008.1080p.BrRip.x264.YIFY.mp4"); count += 1
    junk(im1 / "Iron.Man.2008.1080p.BrRip.x264.YIFY.srt")
    junk(im1 / "WWW.YIFY-TORRENTS.COM.jpg")

    # Iron Man 2 (2010)
    im2 = D / "Iron Man 2 (2010) [1080p]"
    mkv(im2 / "Iron.Man.2.2010.1080p.BrRip.x264.YIFY.mp4"); count += 1
    junk(im2 / "Iron.Man.2.2010.1080p.BrRip.x264.YIFY.srt")
    junk(im2 / "WWW.YIFY-TORRENTS.COM.jpg")
    junk(im2 / "Other" / "AhaShare.com.txt")
    junk(im2 / "Other" / "Torrent downloaded from Demonoid.com - Copy.txt")
    junk(im2 / "Other" / "Torrent Downloaded From ExtraTorrent.com.txt")

    # Iron Man 3 (2013)
    im3 = D / "Iron Man 3 (2013) [1080p]"
    mkv(im3 / "Iron.Man.3.2013.1080p.BluRay.x264.YIFY.mp4"); count += 1
    junk(im3 / "WWW.YIFY-TORRENTS.COM.jpg")

    # ========================
    # ANIME MOVIE FOLDER (torrent junk galore)
    # ========================
    jjk0 = D / "Jujutsu.Kaisen.0.(2021).YG"
    mkv(jjk0 / "Jujutsu.Kaisen.0.(2021).(Movie).1080p.BRRip.x264.Dual.YG.mkv"); count += 1
    junk(jjk0 / "InFo YG.nfo")
    junk(jjk0 / "Torrent Downloaded From GattyYG.txt")
    for site in ["1337x.to", "Angietorrents.cc", "ETTVcentral.com", "Glodls.to",
                  "Prostylex.org", "sharefiles.ro", "Thepiratebay.org",
                  "Torrentgalaxy.to "]:
        junk(jjk0 / "Torrent Downloaded From" / f"Torrent Downloaded From {site}.txt")
    junk(jjk0 / "Torrent Downloaded From" / "Torrent_downloaded_from_Demonoid.is_.txt")

    print(f"  Movies: {count} video files created")
    return count


def create_shows():
    """Create all TV show test files in Drop_Shows."""
    D = DROP_SHOWS
    count = 0

    # ========================
    # STANDALONE EPISODES (scene releases)
    # ========================
    for f in [
        "Breaking.Bad.S01E01.Pilot.720p.HDTV.x264-CTU.mkv",
        "La.Casa.de.Papel.S01E01.SPANISH.1080p.NF.WEBRip.DDP5.1.x264-Ao.mkv",
        "Squid.Game.S01E01.Red.Light.Green.Light.1080p.NF.WEB-DL.DDP5.1.x264-TEPES.mkv",
        "SpongeBob.SquarePants.S01E01.Help.Wanted.480p.DVDRip.x264-Encode.mkv",
        "[Erai-raws] Kimetsu no Yaiba - S02E01 [1080p][Multiple Subtitle].mkv",
    ]:
        mkv(D / f); count += 1

    # ========================
    # HAIKYUU!! - Full Series (real-world batch download)
    # ========================
    hkf = D / "Haikyuu!!"

    # OAD
    for f in [
        "[Breeze] Haikyuu!! - OAD [1080p][AV1].mkv",
        "[Breeze] Haikyuu!! Karasuno High School vs. Shiratorizawa Academy - OAD [1080p][AV1].mkv",
        "[Breeze] Haikyuu!! Second Season - OAD [1080p][AV1].mkv",
    ]:
        mkv(hkf / "OAD" / f); count += 1

    # OVA
    for i in range(1, 3):
        mkv(hkf / "OVA" / f"[Breeze] Haikyuu!! Land vs. Air - {i:02d} [1080p BD][AV1][dual audio].mkv"); count += 1

    # Season 1 (25 episodes)
    for i in range(1, 26):
        mkv(hkf / "Season 1" / f"[Breeze] Haikyuu!! - {i:02d} [1080p BD][AV1][dual audio].mkv"); count += 1

    # Season 2 (25 episodes)
    for i in range(1, 26):
        mkv(hkf / "Season 2" / f"[Breeze] Haikyuu!! Second Season - {i:02d} [1080p BD][AV1][dual audio].mkv"); count += 1

    # Season 3 (10 episodes)
    for i in range(1, 11):
        mkv(hkf / "Season 3" / f"[Breeze] Haikyuu!! Karasuno High School vs. Shiratorizawa Academy - {i:02d} [1080p BD][AV1][dual audio].mkv"); count += 1

    # Season 4 (25 episodes) - Note: eps 1-4 use space, eps 5-25 use dot before S04
    for i in range(1, 5):
        mkv(hkf / "Season 4" / f"[Breeze] Haikyu S04E{i:02d} [1080p BD][AV1][dual audio].mkv"); count += 1
    for i in range(5, 26):
        mkv(hkf / "Season 4" / f"[Breeze] Haikyu.S04E{i:02d} [1080p BD][AV1][dual audio].mkv"); count += 1

    # ========================
    # JUJUTSU KAISEN S01 (with featurettes)
    # ========================
    jjk1 = D / "Jujutsu Kaisen (2020) S01 (1080p BluRay AV1 10bit Opus 2.0 KaNNa)"
    for i in range(1, 25):
        mkv(jjk1 / f"Jujutsu Kaisen (2020) - S01E{i:02d} (1080p BluRay AV1 10bit Opus 2.0 KaNNa).mkv"); count += 1
    # Featurettes (NCOP/NCED - no episode number)
    for f in ["NCED1", "NCED2", "NCOP1", "NCOP2"]:
        mkv(jjk1 / "Featurettes" / f"Jujutsu Kaisen (2020) - S01 - {f}.mkv"); count += 1

    # ========================
    # JUJUTSU KAISEN S02 (version numbers in filenames)
    # ========================
    jjk2 = D / "[Judas] Jujutsu Kaisen (Season 2) [1080p][HEVC x265 10bit][Dual-Audio][Multi-Subs]"
    for i in range(1, 24):
        mkv(jjk2 / f"[Judas] Jujutsu Kaisen - S02E{i:02d}v2.mkv"); count += 1

    # ========================
    # KUNG FU PANDA: THE PAWS OF DESTINY (Hindi + junk)
    # ========================
    kfp = D / "Kung Fu Panda - The Paws of Destiny (2018) S01 WEB-DL 720p [Hindi + English] DD5.1 x265 HEVC"
    for i in range(1, 14):
        mkv(kfp / f"Kung.Fu.Panda.The.Paws.Of.Destiny.S01E{i:02d}.720p.5.1.Hin-Eng.HEVC-KatmovieHD.Tv.mkv"); count += 1
    junk(kfp / "KatmovieHD.Tv.txt")
    junk(kfp / "Katmoviehd.tv.url")

    # ========================
    # SILICON VALLEY - Complete Series S01-S06 (spaces, .mp4)
    # ========================
    sv = D / "Silicon Valley Complete Series (S01 - S06) 1080p 5.1 - 2.0 x264 Phun Psyz"

    # S01 (8 episodes)
    sv_s01 = [
        "Minimum Viable Product", "The Cap Table", "Articles of Incorporation",
        "Fiduciary Duties", "Signaling Risk", "Third Party Insourcing",
        "Proof of Concept", "Optimal Tip-To-Tip Efficiency",
    ]
    for i, title in enumerate(sv_s01, 1):
        mkv(sv / "Season 1" / f"Silicon Valley S01E{i:02d} {title}.mp4"); count += 1

    # S02 (10 episodes)
    sv_s02 = [
        "Sand Hill Shuffle", "Runaway Devaluation", "Bad Money", "The Lady",
        "Server Space", "Homicide", "Adult Content", "White Hat - Black Hat",
        "Binding Arbitration", "Two Days of the Condor",
    ]
    for i, title in enumerate(sv_s02, 1):
        mkv(sv / "Season 2" / f"Silicon Valley S02E{i:02d} {title}.mp4"); count += 1

    # S03 (10 episodes)
    sv_s03 = [
        "Founder Friendly", "Two In The Box", "Meinertzhagens Haversack",
        "Maleant Data Systems Solutions", "The Empty Chair", "Bachmanity Insanity",
        "To Build A Better Beta", "Bachmans Earnings Over-Ride",
        "Daily Active Users", "The Uptick",
    ]
    for i, title in enumerate(sv_s03, 1):
        mkv(sv / "Season 3" / f"Silicon Valley S03E{i:02d} {title}.mp4"); count += 1

    # S04 (10 episodes)
    sv_s04 = [
        "Success Failure", "Terms Of Service", "Intellectual Property",
        "Teambuilding Exercise", "The Blood Boy", "Customer Service",
        "The Patent Troll", "The Keenan Vortex", "Hooli-Con", "Server Error",
    ]
    for i, title in enumerate(sv_s04, 1):
        mkv(sv / "Season 4" / f"Silicon Valley S04E{i:02d} {title}.mp4"); count += 1

    # S05 (8 episodes)
    sv_s05 = [
        "Grow Fast Or Die Slow", "Reorientation", "Chief Operating Officer",
        "Tech Evangelist", "Facial Recognition", "Artificial Emotional Intelligence",
        "Initial Coin Offering", "Fifty-One Percent",
    ]
    for i, title in enumerate(sv_s05, 1):
        mkv(sv / "Season 5" / f"Silicon Valley S05E{i:02d} {title}.mp4"); count += 1

    # S06 (7 episodes)
    sv_s06 = [
        "Artificial Lack of Intelligence", "Blood Money", "Hooli Smokes!",
        "Maximizing Alphaness", "Tethics", "RussFest", "Exit Event",
    ]
    for i, title in enumerate(sv_s06, 1):
        mkv(sv / "Season 6" / f"Silicon Valley S06E{i:02d} {title}.mp4"); count += 1

    # ========================
    # TED LASSO S01-S03 (scene release naming with dots)
    # ========================

    # S01
    tl1 = D / "Ted.Lasso.S01.1080p.ATVP.WEB-DL.DDP5.1.H.264-EniaHD"
    tl_s01 = [
        "Pilot", "Biscuits", "Trent.Crimm.The.Independent",
        "For.the.Children", "Tan.Lines", "Two.Aces",
        "Make.Rebecca.Great.Again", "The.Diamond.Dogs",
        "All.Apologies", "The.Hope.That.Kills.You",
    ]
    for i, title in enumerate(tl_s01, 1):
        mkv(tl1 / f"Ted.Lasso.S01E{i:02d}.{title}.1080p.ATVP.WEB-DL.DDP5.1.H.264-EniaHD.mkv"); count += 1

    # S02
    tl2 = D / "Ted.Lasso.S02.1080p.ATVP.WEB-DL.DDP5.1.H.264-EniaHD"
    tl_s02 = [
        "Goodbye.Earl", "Lavender", "Do.the.Right-est.Thing",
        "Carol.of.the.Bells", "Rainbow", "The.Signal",
        "Headspace", "Man.City", "Beard.After.Hours",
        "No.Weddings.and.a.Funeral", "Midnight.Train.to.Royston",
        "Inverting.the.Pyramid.of.Success",
    ]
    for i, title in enumerate(tl_s02, 1):
        mkv(tl2 / f"Ted.Lasso.S02E{i:02d}.{title}.1080p.ATVP.WEB-DL.DDP5.1.H.264-EniaHD.mkv"); count += 1

    # S03
    tl3 = D / "Ted.Lasso.S03.COMPLETE.1080p.ATVP.WEBRip.DDP5.1.x264-NTb[TGx]"
    tl_s03 = [
        "Smells.Like.Mean.Spirit", "I.Dont.Want.to.Go.to.Chelsea",
        "4-5-1", "Big.Week", "Signs", "Sunflowers",
        "The.Strings.That.Bind.Us", "Well.Never.Have.Paris",
        "La.Locker.Room.Aux.Folles", "International.Break",
        "Mom.City", "So.Long.Farewell",
    ]
    for i, title in enumerate(tl_s03, 1):
        mkv(tl3 / f"Ted.Lasso.S03E{i:02d}.{title}.1080p.ATVP.WEB-DL.DDP5.1.H.264-NTb.mkv"); count += 1
    junk(tl3 / "NEW upcoming releases by Xclusive.txt")
    junk(tl3 / "[TGx]Downloaded from torrentgalaxy.to .txt")

    # ========================
    # THE WIRE - Deep nesting with bare filename
    # ========================
    wire = D / "The.Wire.S01.1080p.BluRay.REMUX.AVC.DTS-HD.MA.5.1-EPSiLON" / "Season 1"
    mkv(wire / "01.mkv"); count += 1

    print(f"  Shows:  {count} video files created")
    return count


def main():
    clean = "--clean" in sys.argv

    if clean:
        print("Cleaning Drop folders...")
        for d in [DROP_MOVIES, DROP_SHOWS]:
            if d.exists():
                shutil.rmtree(d)
        print("  Done.")

    DROP_MOVIES.mkdir(parents=True, exist_ok=True)
    DROP_SHOWS.mkdir(parents=True, exist_ok=True)

    print(f"\nDrop_Movies: {DROP_MOVIES}")
    print(f"Drop_Shows:  {DROP_SHOWS}\n")
    print("Creating test files...")

    m = create_movies()
    s = create_shows()

    print(f"\nTotal: {m + s} video files ({m} movies, {s} shows)")
    print("Test generation complete.")


if __name__ == "__main__":
    main()
