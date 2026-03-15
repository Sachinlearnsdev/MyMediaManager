#!/usr/bin/env python3
"""
Stress Test Generator for MyMediaManager
Creates 700+ test files across all categories and regional cinema.
"""
import os
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DROP_SHOWS = BASE / "Drop_Shows"
DROP_MOVIES = BASE / "Drop_Movies"

DROP_SHOWS.mkdir(exist_ok=True)
DROP_MOVIES.mkdir(exist_ok=True)

def touch(folder, name):
    (folder / name).write_bytes(b'\x00' * 5)

# ============================================================================
# MOVIES — Drop_Movies (400+ files)
# ============================================================================

# --- 100 Hollywood Movies (various decades, genres, formats) ---
hollywood = [
    # Classic
    "The.Godfather.1972.REMASTERED.1080p.BluRay.x264-SPARKS.mkv",
    "The.Shawshank.Redemption.1994.1080p.BluRay.x264.mkv",
    "Schindlers.List.1993.1080p.BluRay.x265.HEVC.mkv",
    "Pulp.Fiction.1994.REMASTERED.2160p.UHD.BluRay.mkv",
    "The.Dark.Knight.2008.1080p.BluRay.x264-DEMAND.mkv",
    "Fight.Club.1999.REMASTERED.1080p.BluRay.x264.mkv",
    "Forrest.Gump.1994.REMASTERED.1080p.BluRay.mkv",
    "Goodfellas.1990.REMASTERED.1080p.BluRay.x264.mkv",
    "The.Matrix.1999.REMASTERED.2160p.UHD.BluRay.x265.mkv",
    "Se7en.1995.REMASTERED.1080p.BluRay.x264.mkv",
    # 2000s
    "Gladiator.2000.EXTENDED.1080p.BluRay.x264.mkv",
    "The.Lord.of.the.Rings.The.Fellowship.of.the.Ring.2001.EXTENDED.1080p.BluRay.mkv",
    "The.Lord.of.the.Rings.The.Two.Towers.2002.EXTENDED.1080p.BluRay.mkv",
    "The.Lord.of.the.Rings.The.Return.of.the.King.2003.EXTENDED.1080p.BluRay.mkv",
    "Inception.2010.1080p.BluRay.x264-DEMAND.mkv",
    "The.Departed.2006.1080p.BluRay.x264.mkv",
    "No.Country.for.Old.Men.2007.1080p.BluRay.x264.mkv",
    "There.Will.Be.Blood.2007.1080p.BluRay.x264.mkv",
    "The.Social.Network.2010.1080p.BluRay.x264.mkv",
    "Inglourious.Basterds.2009.1080p.BluRay.x264.mkv",
    # 2010s
    "Interstellar.2014.IMAX.2160p.UHD.BluRay.x265.mkv",
    "Mad.Max.Fury.Road.2015.BLACK.AND.CHROME.1080p.BluRay.mkv",
    "Whiplash.2014.1080p.BluRay.x264.mkv",
    "The.Revenant.2015.1080p.BluRay.x264.mkv",
    "La.La.Land.2016.1080p.BluRay.x264.mkv",
    "Get.Out.2017.1080p.BluRay.x264.mkv",
    "Dunkirk.2017.IMAX.1080p.BluRay.x264.mkv",
    "Blade.Runner.2049.2017.1080p.BluRay.x264.mkv",
    "Parasite.2019.1080p.BluRay.x264-YTS.mkv",
    "Joker.2019.1080p.BluRay.x264.mkv",
    # 2020s Recent
    "Dune.2021.IMAX.2160p.WEB-DL.DDP5.1.Atmos.mkv",
    "Dune.Part.Two.2024.IMAX.2160p.WEB-DL.mkv",
    "Oppenheimer.2023.IMAX.1080p.BluRay.x264.mkv",
    "Everything.Everywhere.All.at.Once.2022.1080p.BluRay.mkv",
    "Top.Gun.Maverick.2022.IMAX.1080p.BluRay.x264.mkv",
    "The.Batman.2022.IMAX.1080p.BluRay.x264.mkv",
    "Spider-Man.No.Way.Home.2021.EXTENDED.1080p.BluRay.mkv",
    "The.Whale.2022.1080p.BluRay.x264.mkv",
    "Killers.of.the.Flower.Moon.2023.1080p.WEB-DL.mkv",
    "Barbie.2023.1080p.BluRay.x264.mkv",
    # Horror
    "Hereditary.2018.1080p.BluRay.x264.mkv",
    "Midsommar.2019.DIRECTORS.CUT.1080p.BluRay.mkv",
    "The.Exorcist.1973.DIRECTORS.CUT.1080p.BluRay.mkv",
    "A.Quiet.Place.2018.1080p.BluRay.x264.mkv",
    "Us.2019.1080p.BluRay.x264.mkv",
    "The.Shining.1980.REMASTERED.1080p.BluRay.mkv",
    "It.2017.1080p.BluRay.x264.mkv",
    "Nope.2022.1080p.BluRay.x264.mkv",
    "Barbarian.2022.1080p.WEB-DL.mkv",
    "Smile.2022.1080p.BluRay.x264.mkv",
    # Sci-Fi
    "Arrival.2016.1080p.BluRay.x264.mkv",
    "Ex.Machina.2014.1080p.BluRay.x264.mkv",
    "Annihilation.2018.1080p.WEB-DL.mkv",
    "The.Martian.2015.EXTENDED.1080p.BluRay.mkv",
    "Gravity.2013.1080p.BluRay.x264.mkv",
    "Edge.of.Tomorrow.2014.1080p.BluRay.x264.mkv",
    "Tenet.2020.IMAX.1080p.BluRay.x264.mkv",
    "Ad.Astra.2019.IMAX.1080p.BluRay.mkv",
    "Prometheus.2012.1080p.BluRay.x264.mkv",
    "Alien.Romulus.2024.1080p.WEB-DL.mkv",
    # Comedy
    "The.Grand.Budapest.Hotel.2014.1080p.BluRay.mkv",
    "Superbad.2007.UNRATED.1080p.BluRay.mkv",
    "The.Hangover.2009.1080p.BluRay.x264.mkv",
    "Bridesmaids.2011.UNRATED.1080p.BluRay.mkv",
    "Knives.Out.2019.1080p.BluRay.x264.mkv",
    "Glass.Onion.A.Knives.Out.Mystery.2022.1080p.WEB-DL.mkv",
    "Jojo.Rabbit.2019.1080p.BluRay.x264.mkv",
    "The.Menu.2022.1080p.BluRay.x264.mkv",
    "Palm.Springs.2020.1080p.WEB-DL.mkv",
    "Booksmart.2019.1080p.BluRay.x264.mkv",
    # Action / Thriller
    "John.Wick.Chapter.4.2023.1080p.BluRay.x264.mkv",
    "Mission.Impossible.Dead.Reckoning.Part.One.2023.1080p.BluRay.mkv",
    "The.Equalizer.3.2023.1080p.BluRay.mkv",
    "Extraction.2020.1080p.WEB-DL.mkv",
    "Nobody.2021.1080p.BluRay.x264.mkv",
    "Bullet.Train.2022.1080p.BluRay.mkv",
    "The.Gray.Man.2022.1080p.WEB-DL.mkv",
    "Fast.X.2023.1080p.BluRay.x264.mkv",
    "Furiosa.A.Mad.Max.Saga.2024.1080p.WEB-DL.mkv",
    "The.Beekeeper.2024.1080p.WEB-DL.mkv",
    # Animated (NOT anime — should go to Movies, not Anime/Movies)
    "Inside.Out.2.2024.1080p.WEB-DL.mkv",
    "Toy.Story.1995.REMASTERED.1080p.BluRay.mkv",
    "WALL-E.2008.1080p.BluRay.x264.mkv",
    "Coco.2017.1080p.BluRay.x264.mkv",
    "Ratatouille.2007.1080p.BluRay.x264.mkv",
    "Shrek.2001.1080p.BluRay.x264.mkv",
    "Kung.Fu.Panda.4.2024.1080p.WEB-DL.mkv",
    "The.Super.Mario.Bros.Movie.2023.1080p.BluRay.mkv",
    "Elemental.2023.1080p.BluRay.mkv",
    "Puss.in.Boots.The.Last.Wish.2022.1080p.BluRay.mkv",
    # Tricky filenames / edge cases
    "1917.2019.1080p.BluRay.x264.mkv",  # starts with number
    "2001.A.Space.Odyssey.1968.1080p.BluRay.mkv",  # year-like title
    "12.Angry.Men.1957.1080p.BluRay.mkv",  # number prefix
    "500.Days.of.Summer.2009.1080p.BluRay.mkv",  # number prefix
    "Se7en.1995.1080p.BluRay.x264-SPARKS.mkv",  # leet speak title (dupe test)
    "CODA.2021.1080p.WEB-DL.mkv",  # all caps short title
    "RRR.2022.1080p.NF.WEB-DL.DDP5.1.mkv",  # Indian movie in Hollywood format
    "The.Banshees.of.Inisherin.2022.1080p.BluRay.mkv",
    "Poor.Things.2023.1080p.BluRay.mkv",
    "Past.Lives.2023.1080p.WEB-DL.mkv",
]

# --- 100 Bollywood Movies ---
bollywood = [
    # Classics
    "Sholay.1975.REMASTERED.1080p.BluRay.mkv",
    "Mughal-E-Azam.1960.COLORIZED.1080p.BluRay.mkv",
    "Dilwale.Dulhania.Le.Jayenge.1995.1080p.BluRay.mkv",
    "Lagaan.Once.Upon.a.Time.in.India.2001.1080p.BluRay.mkv",
    "Dil.Chahta.Hai.2001.1080p.WEB-DL.mkv",
    "Swades.2004.1080p.BluRay.x264.mkv",
    "Rang.De.Basanti.2006.1080p.BluRay.mkv",
    "Chak.De.India.2007.1080p.BluRay.mkv",
    "Taare.Zameen.Par.2007.1080p.BluRay.mkv",
    "3.Idiots.2009.1080p.BluRay.x264.mkv",
    # 2010s
    "Gangs.of.Wasseypur.2012.1080p.BluRay.mkv",
    "Gangs.of.Wasseypur.Part.2.2012.1080p.BluRay.mkv",
    "Zindagi.Na.Milegi.Dobara.2011.1080p.BluRay.mkv",
    "Barfi.2012.1080p.BluRay.mkv",
    "PK.2014.1080p.BluRay.x264.mkv",
    "Queen.2013.1080p.BluRay.mkv",
    "Bajrangi.Bhaijaan.2015.1080p.BluRay.mkv",
    "Dangal.2016.1080p.BluRay.x264.mkv",
    "Pink.2016.1080p.WEB-DL.mkv",
    "Andhadhun.2018.1080p.BluRay.mkv",
    "Tumbbad.2018.1080p.WEB-DL.mkv",
    "Article.15.2019.1080p.WEB-DL.mkv",
    "Gully.Boy.2019.1080p.WEB-DL.mkv",
    "Super.30.2019.1080p.WEB-DL.mkv",
    "War.2019.1080p.BluRay.mkv",
    "Uri.The.Surgical.Strike.2019.1080p.BluRay.mkv",
    "Kabir.Singh.2019.1080p.WEB-DL.mkv",
    "Chhichhore.2019.1080p.WEB-DL.mkv",
    "Bala.2019.1080p.WEB-DL.mkv",
    "Ludo.2020.1080p.NF.WEB-DL.mkv",
    # 2020s
    "Drishyam.2.2022.1080p.WEB-DL.mkv",
    "Gangubai.Kathiawadi.2022.1080p.NF.WEB-DL.mkv",
    "The.Kashmir.Files.2022.1080p.WEB-DL.mkv",
    "Brahmastra.Part.One.Shiva.2022.1080p.WEB-DL.mkv",
    "Vikram.Vedha.2022.1080p.WEB-DL.mkv",
    "An.Action.Hero.2022.1080p.WEB-DL.mkv",
    "Pathaan.2023.1080p.WEB-DL.mkv",
    "Rocky.Aur.Rani.Kii.Prem.Kahaani.2023.1080p.WEB-DL.mkv",
    "Jawan.2023.1080p.NF.WEB-DL.mkv",
    "Dunki.2023.1080p.WEB-DL.mkv",
    "12th.Fail.2023.1080p.WEB-DL.mkv",
    "Sam.Bahadur.2023.1080p.WEB-DL.mkv",
    "Animal.2023.1080p.WEB-DL.mkv",
    "Tiger.3.2023.1080p.WEB-DL.mkv",
    "Teri.Baaton.Mein.Aisa.Uljha.Jiya.2024.1080p.WEB-DL.mkv",
    "Fighter.2024.1080p.WEB-DL.mkv",
    "Crew.2024.1080p.WEB-DL.mkv",
    "Shaitaan.2024.1080p.WEB-DL.mkv",
    "Laapataa.Ladies.2024.1080p.NF.WEB-DL.mkv",
    "Srikanth.2024.1080p.WEB-DL.mkv",
    # Horror / Thriller
    "Stree.2018.1080p.WEB-DL.mkv",
    "Stree.2.2024.1080p.WEB-DL.mkv",
    "Bhool.Bhulaiyaa.2.2022.1080p.WEB-DL.mkv",
    "Bhool.Bhulaiyaa.3.2024.1080p.WEB-DL.mkv",
    "Roohi.2021.1080p.WEB-DL.mkv",
    "Munjya.2024.1080p.WEB-DL.mkv",
    "Pari.2018.1080p.WEB-DL.mkv",
    "Raaz.Reboot.2016.1080p.WEB-DL.mkv",
    "1920.2008.1080p.WEB-DL.mkv",
    "Darna.Mana.Hai.2003.720p.WEB-DL.mkv",
    # Action
    "Singham.2011.1080p.BluRay.mkv",
    "Singham.Returns.2014.1080p.BluRay.mkv",
    "Simmba.2018.1080p.WEB-DL.mkv",
    "Sooryavanshi.2021.1080p.WEB-DL.mkv",
    "Dabangg.2010.1080p.BluRay.mkv",
    "Dhoom.2004.1080p.BluRay.mkv",
    "Dhoom.2.2006.1080p.BluRay.mkv",
    "Dhoom.3.2013.1080p.BluRay.mkv",
    "Bang.Bang.2014.1080p.BluRay.mkv",
    "Race.3.2018.1080p.WEB-DL.mkv",
    # Romance / Drama
    "Yeh.Jawaani.Hai.Deewani.2013.1080p.BluRay.mkv",
    "Ae.Dil.Hai.Mushkil.2016.1080p.BluRay.mkv",
    "Dear.Zindagi.2016.1080p.BluRay.mkv",
    "Raanjhanaa.2013.1080p.BluRay.mkv",
    "Lootera.2013.1080p.BluRay.mkv",
    "Rockstar.2011.1080p.BluRay.mkv",
    "Tamasha.2015.1080p.BluRay.mkv",
    "Raees.2017.1080p.BluRay.mkv",
    "Pad.Man.2018.1080p.WEB-DL.mkv",
    "Sanju.2018.1080p.WEB-DL.mkv",
    # Biographical
    "Bhaag.Milkha.Bhaag.2013.1080p.BluRay.mkv",
    "Mary.Kom.2014.1080p.BluRay.mkv",
    "Neerja.2016.1080p.BluRay.mkv",
    "MS.Dhoni.The.Untold.Story.2016.1080p.BluRay.mkv",
    "Manikarnika.The.Queen.of.Jhansi.2019.1080p.WEB-DL.mkv",
    "83.2021.1080p.WEB-DL.mkv",
    "Sardar.Udham.2021.1080p.AMZN.WEB-DL.mkv",
    "The.Zoya.Factor.2019.1080p.WEB-DL.mkv",
    "Chhapaak.2020.1080p.WEB-DL.mkv",
    "Gunjan.Saxena.The.Kargil.Girl.2020.1080p.NF.WEB-DL.mkv",
    # Tricky
    "Ghajini.2008.1080p.BluRay.mkv",
    "Don.2006.1080p.BluRay.mkv",
    "Don.2.2011.1080p.BluRay.mkv",
    "OMG.Oh.My.God.2012.1080p.BluRay.mkv",
    "ABCD.Any.Body.Can.Dance.2013.1080p.BluRay.mkv",
    "NH10.2015.1080p.WEB-DL.mkv",
    "Section.375.2019.1080p.WEB-DL.mkv",
    "99.Songs.2021.1080p.WEB-DL.mkv",
    "Haseen.Dillruba.2021.1080p.NF.WEB-DL.mkv",
    "Jalsa.2022.1080p.AMZN.WEB-DL.mkv",
]

# --- 100 Tollywood / South Indian Movies ---
tollywood = [
    # Telugu Blockbusters
    "Baahubali.The.Beginning.2015.1080p.BluRay.x264.mkv",
    "Baahubali.2.The.Conclusion.2017.1080p.BluRay.x264.mkv",
    "RRR.2022.1080p.NF.WEB-DL.Hindi.mkv",
    "Pushpa.The.Rise.2021.1080p.WEB-DL.mkv",
    "Pushpa.The.Rule.2024.1080p.WEB-DL.mkv",
    "KGF.Chapter.1.2018.1080p.BluRay.mkv",
    "KGF.Chapter.2.2022.1080p.WEB-DL.mkv",
    "Eega.2012.1080p.BluRay.mkv",
    "Magadheera.2009.1080p.BluRay.mkv",
    "Arjun.Reddy.2017.1080p.WEB-DL.mkv",
    "Ala.Vaikunthapurramuloo.2020.1080p.WEB-DL.mkv",
    "Jersey.2019.1080p.WEB-DL.mkv",
    "Rangasthalam.2018.1080p.WEB-DL.mkv",
    "Sye.Raa.Narasimha.Reddy.2019.1080p.BluRay.mkv",
    "Saaho.2019.1080p.BluRay.mkv",
    "Bheemla.Nayak.2022.1080p.WEB-DL.mkv",
    "Sita.Ramam.2022.1080p.WEB-DL.mkv",
    "Ante.Sundaraniki.2022.1080p.WEB-DL.mkv",
    "Major.2022.1080p.WEB-DL.mkv",
    "HIT.The.First.Case.2020.1080p.WEB-DL.mkv",
    "Salaar.Part.1.Ceasefire.2023.1080p.WEB-DL.mkv",
    "Hi.Nanna.2023.1080p.WEB-DL.mkv",
    "Dasara.2023.1080p.WEB-DL.mkv",
    "Kushi.2023.1080p.WEB-DL.mkv",
    "Guntur.Kaaram.2024.1080p.WEB-DL.mkv",
    # Tamil
    "Vikram.2022.1080p.WEB-DL.mkv",
    "Ponniyin.Selvan.I.2022.1080p.WEB-DL.mkv",
    "Ponniyin.Selvan.II.2023.1080p.WEB-DL.mkv",
    "Jailer.2023.1080p.WEB-DL.mkv",
    "Leo.2023.1080p.WEB-DL.mkv",
    "Master.2021.1080p.WEB-DL.mkv",
    "Soorarai.Pottru.2020.1080p.AMZN.WEB-DL.mkv",
    "Karnan.2021.1080p.WEB-DL.mkv",
    "Jai.Bhim.2021.1080p.WEB-DL.mkv",
    "Sarpatta.Parambarai.2021.1080p.AMZN.WEB-DL.mkv",
    "Asuran.2019.1080p.WEB-DL.mkv",
    "Super.Deluxe.2019.1080p.WEB-DL.mkv",
    "96.2018.1080p.WEB-DL.mkv",
    "Ratsasan.2018.1080p.WEB-DL.mkv",
    "Vada.Chennai.2018.1080p.WEB-DL.mkv",
    "Kaithi.2019.1080p.WEB-DL.mkv",
    "Indian.2.2024.1080p.WEB-DL.mkv",
    "Amaran.2024.1080p.WEB-DL.mkv",
    "GOAT.2024.1080p.WEB-DL.mkv",
    "Maharaja.2024.1080p.WEB-DL.mkv",
    "Demonte.Colony.2.2024.1080p.WEB-DL.mkv",
    "Raayan.2024.1080p.WEB-DL.mkv",
    "Aranmanai.4.2024.1080p.WEB-DL.mkv",
    "Lover.2024.1080p.WEB-DL.mkv",
    "Star.2024.1080p.WEB-DL.mkv",
    # Kannada
    "Kantara.2022.1080p.WEB-DL.mkv",
    "777.Charlie.2022.1080p.WEB-DL.mkv",
    "Ugramm.2014.1080p.WEB-DL.mkv",
    "Kirik.Party.2016.1080p.WEB-DL.mkv",
    "Lucia.2013.1080p.WEB-DL.mkv",
    "Garuda.Gamana.Vrishabha.Vahana.2021.1080p.WEB-DL.mkv",
    "Vikrant.Rona.2022.1080p.WEB-DL.mkv",
    "Martin.2024.1080p.WEB-DL.mkv",
    "Toby.2023.1080p.WEB-DL.mkv",
    "Bairagee.2024.1080p.WEB-DL.mkv",
    # Malayalam
    "Drishyam.2013.1080p.BluRay.mkv",
    "Premam.2015.1080p.BluRay.mkv",
    "Bangalore.Days.2014.1080p.WEB-DL.mkv",
    "Kumbalangi.Nights.2019.1080p.WEB-DL.mkv",
    "Virus.2019.1080p.WEB-DL.mkv",
    "The.Great.Indian.Kitchen.2021.1080p.WEB-DL.mkv",
    "Minnal.Murali.2021.1080p.NF.WEB-DL.mkv",
    "Jaya.Jaya.Jaya.Jaya.Hey.2022.1080p.WEB-DL.mkv",
    "2018.Everyone.Is.a.Hero.2023.1080p.WEB-DL.mkv",
    "Manjummel.Boys.2024.1080p.WEB-DL.mkv",
    "Aadujeevitham.The.Goat.Life.2024.1080p.WEB-DL.mkv",
    "Aavesham.2024.1080p.WEB-DL.mkv",
    "Turbo.2024.1080p.WEB-DL.mkv",
    "Bramayugam.2024.1080p.WEB-DL.mkv",
    "Premalu.2024.1080p.WEB-DL.mkv",
    # Bengali
    "Pather.Panchali.1955.Criterion.1080p.BluRay.mkv",
    "Charulata.1964.Criterion.1080p.BluRay.mkv",
    "Kahaani.2012.1080p.BluRay.mkv",
    "Aparajito.2022.1080p.WEB-DL.mkv",
    "Belashuru.2022.1080p.WEB-DL.mkv",
    # Marathi
    "Sairat.2016.1080p.WEB-DL.mkv",
    "Court.2014.1080p.WEB-DL.mkv",
    "Natsamrat.2016.1080p.WEB-DL.mkv",
    "Jhund.2022.1080p.WEB-DL.mkv",
    "Vaalvi.2023.1080p.WEB-DL.mkv",
    # Punjabi
    "Jatt.and.Juliet.2012.1080p.WEB-DL.mkv",
    "Carry.on.Jatta.2012.1080p.WEB-DL.mkv",
    "Angrej.2015.1080p.WEB-DL.mkv",
    "Chal.Mera.Putt.2019.1080p.WEB-DL.mkv",
    "Honsla.Rakh.2021.1080p.WEB-DL.mkv",
    # Gujarati / Assamese / Others
    "Village.Rockstars.2017.1080p.WEB-DL.mkv",
    "Hellaro.2019.1080p.WEB-DL.mkv",
    "Reva.2018.1080p.WEB-DL.mkv",
    "Wrong.Side.Raju.2016.1080p.WEB-DL.mkv",
    "Chhello.Show.2022.1080p.WEB-DL.mkv",
    # Cross-industry edge cases
    "Devara.Part.1.2024.1080p.WEB-DL.mkv",
    "Kalki.2898.AD.2024.1080p.WEB-DL.mkv",
    "Stree.2.2024.1080p.WEB-DL.Hindi.mkv",
    "Singham.Again.2024.1080p.WEB-DL.mkv",
    "Bhool.Bhulaiyaa.3.2024.1080p.WEB-DL.Hindi.mkv",
]

# --- 100 International / Other Regional Movies ---
international = [
    # Korean
    "Oldboy.2003.1080p.BluRay.x264.mkv",
    "Memories.of.Murder.2003.1080p.BluRay.mkv",
    "The.Handmaiden.2016.EXTENDED.1080p.BluRay.mkv",
    "Train.to.Busan.2016.1080p.BluRay.mkv",
    "Burning.2018.1080p.BluRay.mkv",
    "Decision.to.Leave.2022.1080p.WEB-DL.mkv",
    "The.Wailing.2016.1080p.BluRay.mkv",
    "I.Saw.the.Devil.2010.1080p.BluRay.mkv",
    "A.Taxi.Driver.2017.1080p.BluRay.mkv",
    "Exhuma.2024.1080p.WEB-DL.mkv",
    # Japanese (Live Action - NOT anime)
    "Shoplifters.2018.1080p.BluRay.mkv",
    "Drive.My.Car.2021.1080p.BluRay.mkv",
    "Departures.2008.1080p.BluRay.mkv",
    "Battle.Royale.2000.1080p.BluRay.mkv",
    "Audition.1999.1080p.BluRay.mkv",
    "13.Assassins.2010.1080p.BluRay.mkv",
    "Ringu.1998.1080p.BluRay.mkv",
    "Godzilla.Minus.One.2023.1080p.WEB-DL.mkv",
    "Perfect.Days.2023.1080p.WEB-DL.mkv",
    "The.Boy.and.the.Heron.2023.1080p.WEB-DL.mkv",  # Miyazaki but live action category test
    # Chinese / Hong Kong
    "In.the.Mood.for.Love.2000.Criterion.1080p.BluRay.mkv",
    "Crouching.Tiger.Hidden.Dragon.2000.1080p.BluRay.mkv",
    "Hero.2002.1080p.BluRay.mkv",
    "Infernal.Affairs.2002.1080p.BluRay.mkv",
    "Kung.Fu.Hustle.2004.1080p.BluRay.mkv",
    "Ip.Man.2008.1080p.BluRay.mkv",
    "The.Wandering.Earth.2019.1080p.WEB-DL.mkv",
    "The.Wandering.Earth.II.2023.1080p.WEB-DL.mkv",
    "Full.River.Red.2023.1080p.WEB-DL.mkv",
    "Creation.of.the.Gods.I.2023.1080p.WEB-DL.mkv",
    # French
    "Amelie.2001.1080p.BluRay.mkv",
    "The.Intouchables.2011.1080p.BluRay.mkv",
    "Blue.Is.the.Warmest.Color.2013.1080p.BluRay.mkv",
    "Portrait.of.a.Lady.on.Fire.2019.1080p.BluRay.mkv",
    "Anatomy.of.a.Fall.2023.1080p.WEB-DL.mkv",
    "The.Count.of.Monte.Cristo.2024.1080p.WEB-DL.mkv",
    "Les.Miserables.2019.1080p.BluRay.mkv",
    "The.Artist.2011.1080p.BluRay.mkv",
    "Raw.2016.1080p.BluRay.mkv",
    "Titane.2021.1080p.BluRay.mkv",
    # Spanish / Latin American
    "Pans.Labyrinth.2006.1080p.BluRay.mkv",
    "The.Orphanage.2007.1080p.BluRay.mkv",
    "Roma.2018.1080p.NF.WEB-DL.mkv",
    "Y.Tu.Mama.Tambien.2001.1080p.BluRay.mkv",
    "The.Secret.in.Their.Eyes.2009.1080p.BluRay.mkv",
    "Wild.Tales.2014.1080p.BluRay.mkv",
    "A.Fantastic.Woman.2017.1080p.BluRay.mkv",
    "Pain.and.Glory.2019.1080p.BluRay.mkv",
    "Society.of.the.Snow.2023.1080p.NF.WEB-DL.mkv",
    "The.Platform.2019.1080p.NF.WEB-DL.mkv",
    # German / Scandinavian
    "The.Lives.of.Others.2006.1080p.BluRay.mkv",
    "Run.Lola.Run.1998.1080p.BluRay.mkv",
    "Downfall.2004.1080p.BluRay.mkv",
    "All.Quiet.on.the.Western.Front.2022.1080p.NF.WEB-DL.mkv",
    "The.Zone.of.Interest.2023.1080p.WEB-DL.mkv",
    "Let.the.Right.One.In.2008.1080p.BluRay.mkv",
    "The.Hunt.2012.1080p.BluRay.mkv",
    "Another.Round.2020.1080p.BluRay.mkv",
    "The.Worst.Person.in.the.World.2021.1080p.BluRay.mkv",
    "Triangle.of.Sadness.2022.1080p.WEB-DL.mkv",
    # Iranian / Middle Eastern
    "A.Separation.2011.1080p.BluRay.mkv",
    "The.Salesman.2016.1080p.BluRay.mkv",
    "About.Elly.2009.1080p.BluRay.mkv",
    "A.Hero.2021.1080p.WEB-DL.mkv",
    "Holy.Spider.2022.1080p.WEB-DL.mkv",
    # Turkish
    "Winter.Sleep.2014.1080p.BluRay.mkv",
    "Once.Upon.a.Time.in.Anatolia.2011.1080p.BluRay.mkv",
    "Miracle.in.Cell.No.7.2019.1080p.NF.WEB-DL.mkv",
    "Muslum.2018.1080p.WEB-DL.mkv",
    "Yol.1982.1080p.BluRay.mkv",
    # Thai / Southeast Asian
    "Uncle.Boonmee.Who.Can.Recall.His.Past.Lives.2010.1080p.BluRay.mkv",
    "Ong-Bak.The.Thai.Warrior.2003.1080p.BluRay.mkv",
    "Bad.Genius.2017.1080p.BluRay.mkv",
    "The.Medium.2021.1080p.WEB-DL.mkv",
    "Pee.Mak.2013.1080p.WEB-DL.mkv",
    # African
    "Tsotsi.2005.1080p.WEB-DL.mkv",
    "Atlantics.2019.1080p.NF.WEB-DL.mkv",
    "The.Boy.Who.Harnessed.the.Wind.2019.1080p.NF.WEB-DL.mkv",
    "Moolaade.2004.1080p.WEB-DL.mkv",
    "Rafiki.2018.1080p.WEB-DL.mkv",
    # Russian / Eastern European
    "Leviathan.2014.1080p.BluRay.mkv",
    "Loveless.2017.1080p.BluRay.mkv",
    "Come.and.See.1985.REMASTERED.1080p.BluRay.mkv",
    "Stalker.1979.Criterion.1080p.BluRay.mkv",
    "Ida.2013.1080p.BluRay.mkv",
    "Cold.War.2018.1080p.BluRay.mkv",
    # Australian / NZ
    "Mad.Max.1979.1080p.BluRay.mkv",
    "The.Babadook.2014.1080p.BluRay.mkv",
    "Lion.2016.1080p.BluRay.mkv",
    "Rabbit-Proof.Fence.2002.1080p.WEB-DL.mkv",
    "Hunt.for.the.Wilderpeople.2016.1080p.BluRay.mkv",
    # Edge cases - year-like names, special chars
    "2046.2004.1080p.BluRay.mkv",
    "8.1-2.1963.Criterion.1080p.BluRay.mkv",
    "Yi.Yi.2000.Criterion.1080p.BluRay.mkv",
    "Capernaum.2018.1080p.BluRay.mkv",
    "The.Bandit.1996.REMASTERED.1080p.BluRay.mkv",
]

# ============================================================================
# TV SHOWS — Drop_Shows
# ============================================================================

# --- 30 Indian TV Shows ---
indian_tv = [
    "Panchayat.S03E01.1080p.AMZN.WEB-DL.mkv",
    "Panchayat.S03E02.1080p.AMZN.WEB-DL.mkv",
    "Mirzapur.S03E01.1080p.AMZN.WEB-DL.mkv",
    "Mirzapur.S03E02.1080p.AMZN.WEB-DL.mkv",
    "The.Family.Man.S02E01.1080p.AMZN.WEB-DL.mkv",
    "The.Family.Man.S02E02.1080p.AMZN.WEB-DL.mkv",
    "Sacred.Games.S01E01.1080p.NF.WEB-DL.mkv",
    "Sacred.Games.S02E01.1080p.NF.WEB-DL.mkv",
    "Scam.1992.The.Harshad.Mehta.Story.S01E01.1080p.WEB-DL.mkv",
    "Paatal.Lok.S01E01.1080p.AMZN.WEB-DL.mkv",
    "Delhi.Crime.S01E01.1080p.NF.WEB-DL.mkv",
    "Delhi.Crime.S02E01.1080p.NF.WEB-DL.mkv",
    "Kota.Factory.S02E01.1080p.NF.WEB-DL.mkv",
    "TVF.Pitchers.S02E01.1080p.WEB-DL.mkv",
    "Rocket.Boys.S01E01.1080p.WEB-DL.mkv",
    "Made.in.Heaven.S02E01.1080p.AMZN.WEB-DL.mkv",
    "Breathe.Into.the.Shadows.S02E01.1080p.AMZN.WEB-DL.mkv",
    "Asur.S02E01.1080p.WEB-DL.mkv",
    "Rana.Naidu.S01E01.1080p.NF.WEB-DL.mkv",
    "Jubilee.S01E01.1080p.AMZN.WEB-DL.mkv",
    "Guns.and.Gulaabs.S01E01.1080p.NF.WEB-DL.mkv",
    "Kohrra.S01E01.1080p.NF.WEB-DL.mkv",
    "Farzi.S01E01.1080p.AMZN.WEB-DL.mkv",
    "Dahaad.S01E01.1080p.AMZN.WEB-DL.mkv",
    "The.Railway.Men.S01E01.1080p.NF.WEB-DL.mkv",
    "Heeramandi.S01E01.1080p.NF.WEB-DL.mkv",
    "Panchayat.S01E01.1080p.AMZN.WEB-DL.mkv",
    "Bandish.Bandits.S02E01.1080p.AMZN.WEB-DL.mkv",
    "Bambai.Meri.Jaan.S01E01.1080p.AMZN.WEB-DL.mkv",
    "Black.Warrant.S01E01.1080p.NF.WEB-DL.mkv",
]

# --- 30 Foreign / International TV Shows ---
foreign_tv = [
    # US Prestige
    "Breaking.Bad.S01E01.720p.BluRay.mkv",
    "Breaking.Bad.S05E16.1080p.BluRay.mkv",
    "Game.of.Thrones.S08E06.1080p.BluRay.mkv",
    "The.Sopranos.S01E01.REMASTERED.1080p.BluRay.mkv",
    "The.Wire.S01E01.REMASTERED.1080p.BluRay.mkv",
    "Better.Call.Saul.S06E13.1080p.WEB-DL.mkv",
    "Succession.S04E10.1080p.WEB-DL.mkv",
    "The.Bear.S03E01.1080p.WEB-DL.mkv",
    "True.Detective.S04E01.1080p.WEB-DL.mkv",
    "Severance.S02E01.1080p.WEB-DL.mkv",
    # Sci-Fi / Fantasy
    "Stranger.Things.S04E09.1080p.NF.WEB-DL.mkv",
    "The.Last.of.Us.S02E01.1080p.WEB-DL.mkv",
    "House.of.the.Dragon.S02E08.1080p.WEB-DL.mkv",
    "The.Mandalorian.S03E08.1080p.DSNP.WEB-DL.mkv",
    "Foundation.S02E10.1080p.WEB-DL.mkv",
    # Korean Drama (should classify as TV)
    "Squid.Game.S02E01.1080p.NF.WEB-DL.mkv",
    "All.of.Us.Are.Dead.S01E01.1080p.NF.WEB-DL.mkv",
    "Vincenzo.S01E01.1080p.NF.WEB-DL.mkv",
    "My.Name.S01E01.1080p.NF.WEB-DL.mkv",
    "Sweet.Home.S03E01.1080p.NF.WEB-DL.mkv",
    # UK
    "Sherlock.S04E03.1080p.BluRay.mkv",
    "Black.Mirror.S06E05.1080p.NF.WEB-DL.mkv",
    "Peaky.Blinders.S06E06.1080p.WEB-DL.mkv",
    "The.Crown.S06E10.1080p.NF.WEB-DL.mkv",
    "Doctor.Who.S14E08.1080p.WEB-DL.mkv",
    # Spanish / Turkish / Other
    "Money.Heist.S05E10.1080p.NF.WEB-DL.mkv",
    "Dark.S03E08.1080p.NF.WEB-DL.mkv",
    "Narcos.S03E10.1080p.NF.WEB-DL.mkv",
    "Elite.S08E08.1080p.NF.WEB-DL.mkv",
    "Lupin.S03E07.1080p.NF.WEB-DL.mkv",
]

# ============================================================================
# CARTOONS — Drop_Shows (40 total: 10 Indian + 30 International)
# ============================================================================
cartoons_indian = [
    "Chhota.Bheem.S01E01.720p.WEB-DL.mkv",
    "Motu.Patlu.S01E01.720p.WEB-DL.mkv",
    "Mighty.Raju.S01E01.720p.WEB-DL.mkv",
    "Kris.Roll.No.21.S01E01.720p.WEB-DL.mkv",
    "Rudra.Boom.Chik.Chik.Boom.S01E01.720p.WEB-DL.mkv",
    "Vir.The.Robot.Boy.S01E01.720p.WEB-DL.mkv",
    "Little.Singham.S01E01.720p.WEB-DL.mkv",
    "Shiva.S01E01.720p.WEB-DL.mkv",
    "Ninja.Hattori.S01E01.720p.WEB-DL.mkv",
    "Oggy.and.the.Cockroaches.S01E01.720p.WEB-DL.mkv",
]

cartoons_international = [
    # Classic US
    "SpongeBob.SquarePants.S13E01.1080p.WEB-DL.mkv",
    "The.Simpsons.S35E22.1080p.WEB-DL.mkv",
    "Family.Guy.S22E20.1080p.WEB-DL.mkv",
    "South.Park.S27E06.1080p.WEB-DL.mkv",
    "Rick.and.Morty.S07E10.1080p.WEB-DL.mkv",
    "Adventure.Time.S10E16.1080p.WEB-DL.mkv",
    "Regular.Show.S08E28.1080p.WEB-DL.mkv",
    "Gravity.Falls.S02E20.1080p.WEB-DL.mkv",
    "Avatar.The.Last.Airbender.S03E21.1080p.BluRay.mkv",
    "The.Legend.of.Korra.S04E13.1080p.WEB-DL.mkv",
    # Adult Animation
    "Arcane.S02E09.1080p.NF.WEB-DL.mkv",
    "Invincible.S02E08.1080p.AMZN.WEB-DL.mkv",
    "Primal.S02E10.1080p.WEB-DL.mkv",
    "Castlevania.S04E10.1080p.NF.WEB-DL.mkv",
    "Helluva.Boss.S02E08.1080p.WEB-DL.mkv",
    "Bob.s.Burgers.S14E22.1080p.WEB-DL.mkv",
    "Futurama.S11E10.1080p.WEB-DL.mkv",
    "King.of.the.Hill.S13E24.1080p.WEB-DL.mkv",
    "Bojack.Horseman.S06E16.1080p.NF.WEB-DL.mkv",
    "Big.Mouth.S08E10.1080p.NF.WEB-DL.mkv",
    # Modern Kids
    "Bluey.S03E52.1080p.WEB-DL.mkv",
    "Peppa.Pig.S09E52.720p.WEB-DL.mkv",
    "PAW.Patrol.S10E26.720p.WEB-DL.mkv",
    "Cocomelon.S07E12.720p.WEB-DL.mkv",
    "Teenage.Mutant.Ninja.Turtles.Mutant.Mayhem.S01E10.1080p.WEB-DL.mkv",
    # European / Other
    "Miraculous.Tales.of.Ladybug.and.Cat.Noir.S05E27.1080p.WEB-DL.mkv",
    "Wakfu.S03E13.1080p.WEB-DL.mkv",
    "Ninjago.Dragons.Rising.S02E16.1080p.WEB-DL.mkv",
    "Transformers.EarthSpark.S02E26.1080p.WEB-DL.mkv",
    "Total.Drama.Island.S02E13.1080p.WEB-DL.mkv",
]

# ============================================================================
# ANIME — Drop_Shows (40 series + 20 movies)
# ============================================================================
anime_series = [
    # Shonen
    "Naruto.Shippuden.S01E01.1080p.BluRay.mkv",
    "One.Piece.S01E1100.1080p.WEB-DL.mkv",
    "Bleach.Thousand.Year.Blood.War.S03E01.1080p.WEB-DL.mkv",
    "Dragon.Ball.Super.S01E131.1080p.WEB-DL.mkv",
    "My.Hero.Academia.S07E21.1080p.WEB-DL.mkv",
    "Jujutsu.Kaisen.S02E23.1080p.WEB-DL.mkv",
    "Demon.Slayer.Kimetsu.no.Yaiba.S04E08.1080p.WEB-DL.mkv",
    "Black.Clover.S01E170.1080p.WEB-DL.mkv",
    "Hunter.x.Hunter.2011.S01E148.1080p.BluRay.mkv",
    "Chainsaw.Man.S01E12.1080p.WEB-DL.mkv",
    # Seinen / Mature
    "Attack.on.Titan.S04E28.1080p.WEB-DL.mkv",
    "Vinland.Saga.S02E24.1080p.WEB-DL.mkv",
    "Tokyo.Ghoul.S01E12.1080p.BluRay.mkv",
    "Berserk.1997.S01E25.1080p.BluRay.mkv",
    "Parasyte.The.Maxim.S01E24.1080p.BluRay.mkv",
    "Monster.S01E74.1080p.BluRay.mkv",
    "Steins.Gate.S01E24.1080p.BluRay.mkv",
    "Psycho-Pass.S01E22.1080p.BluRay.mkv",
    "Death.Note.S01E37.1080p.BluRay.mkv",
    "Code.Geass.S02E25.1080p.BluRay.mkv",
    # Romance / Slice of Life
    "Spy.x.Family.S02E12.1080p.WEB-DL.mkv",
    "Frieren.Beyond.Journeys.End.S01E28.1080p.WEB-DL.mkv",
    "Bocchi.the.Rock.S01E12.1080p.WEB-DL.mkv",
    "Oshi.no.Ko.S02E13.1080p.WEB-DL.mkv",
    "Violet.Evergarden.S01E13.1080p.NF.WEB-DL.mkv",
    "Your.Lie.in.April.S01E22.1080p.BluRay.mkv",
    "Toradora.S01E25.1080p.BluRay.mkv",
    "Kaguya-sama.Love.Is.War.S03E13.1080p.WEB-DL.mkv",
    "Horimiya.S01E13.1080p.WEB-DL.mkv",
    "Skip.and.Loafer.S01E12.1080p.WEB-DL.mkv",
    # Isekai / Fantasy
    "Re.Zero.Starting.Life.in.Another.World.S02E25.1080p.WEB-DL.mkv",
    "Mushoku.Tensei.Jobless.Reincarnation.S02E25.1080p.WEB-DL.mkv",
    "That.Time.I.Got.Reincarnated.as.a.Slime.S03E24.1080p.WEB-DL.mkv",
    "Overlord.S04E13.1080p.WEB-DL.mkv",
    "Konosuba.S03E11.1080p.WEB-DL.mkv",
    # Classics
    "Cowboy.Bebop.S01E26.1080p.BluRay.mkv",
    "Neon.Genesis.Evangelion.S01E26.1080p.BluRay.mkv",
    "Fullmetal.Alchemist.Brotherhood.S01E64.1080p.BluRay.mkv",
    "Samurai.Champloo.S01E26.1080p.BluRay.mkv",
    "Ghost.in.the.Shell.Stand.Alone.Complex.S01E26.1080p.BluRay.mkv",
]

anime_movies = [
    # Studio Ghibli
    "Spirited.Away.2001.1080p.BluRay.x264.mkv",
    "Princess.Mononoke.1997.1080p.BluRay.mkv",
    "My.Neighbor.Totoro.1988.1080p.BluRay.mkv",
    "Howls.Moving.Castle.2004.1080p.BluRay.mkv",
    "Grave.of.the.Fireflies.1988.1080p.BluRay.mkv",
    "Nausicaa.of.the.Valley.of.the.Wind.1984.1080p.BluRay.mkv",
    "Castle.in.the.Sky.1986.1080p.BluRay.mkv",
    "The.Boy.and.the.Heron.2023.1080p.WEB-DL.mkv",
    # Makoto Shinkai
    "Your.Name.2016.1080p.BluRay.x264.mkv",
    "Weathering.with.You.2019.1080p.BluRay.mkv",
    "Suzume.2022.1080p.WEB-DL.mkv",
    "5.Centimeters.Per.Second.2007.1080p.BluRay.mkv",
    # Other
    "Akira.1988.REMASTERED.1080p.BluRay.mkv",
    "Ghost.in.the.Shell.1995.1080p.BluRay.mkv",
    "Perfect.Blue.1997.1080p.BluRay.mkv",
    "Paprika.2006.1080p.BluRay.mkv",
    "A.Silent.Voice.2016.1080p.BluRay.mkv",
    "Dragon.Ball.Super.Broly.2018.1080p.BluRay.mkv",
    "One.Piece.Film.Red.2022.1080p.WEB-DL.mkv",
    "Jujutsu.Kaisen.0.2021.1080p.BluRay.mkv",
]

# ============================================================================
# REALITY TV — Drop_Shows (30 files)
# ============================================================================
reality_tv = [
    # Competition
    "Shark.Tank.S15E01.720p.HDTV.x264.mkv",
    "Shark.Tank.India.S03E01.1080p.WEB-DL.mkv",
    "MasterChef.S14E01.1080p.WEB-DL.mkv",
    "MasterChef.Australia.S16E01.720p.HDTV.mkv",
    "The.Amazing.Race.S36E01.1080p.WEB-DL.mkv",
    "Survivor.S46E01.1080p.WEB-DL.mkv",
    "The.Voice.S25E01.1080p.WEB-DL.mkv",
    "Americas.Got.Talent.S19E01.1080p.WEB-DL.mkv",
    "Big.Brother.S26E01.1080p.WEB-DL.mkv",
    "The.Traitors.S02E01.1080p.WEB-DL.mkv",
    # Dating
    "The.Bachelor.S28E01.1080p.WEB-DL.mkv",
    "Love.Island.S11E01.1080p.WEB-DL.mkv",
    "Too.Hot.to.Handle.S06E01.1080p.NF.WEB-DL.mkv",
    "Love.is.Blind.S07E01.1080p.NF.WEB-DL.mkv",
    "Indian.Matchmaking.S03E01.1080p.NF.WEB-DL.mkv",
    # Survival / Adventure
    "Naked.and.Afraid.S16E01.1080p.WEB-DL.mkv",
    "Alone.S11E01.1080p.WEB-DL.mkv",
    "The.Challenge.S40E01.1080p.WEB-DL.mkv",
    "Bear.Grylls.Running.Wild.S07E01.1080p.WEB-DL.mkv",
    "Race.to.Survive.Alaska.S02E01.1080p.WEB-DL.mkv",
    # Lifestyle / Makeover
    "Queer.Eye.S08E01.1080p.NF.WEB-DL.mkv",
    "The.Great.British.Bake.Off.S14E01.1080p.WEB-DL.mkv",
    "Selling.Sunset.S08E01.1080p.NF.WEB-DL.mkv",
    "Keeping.Up.with.the.Kardashians.S20E01.1080p.WEB-DL.mkv",
    "Below.Deck.S11E01.1080p.WEB-DL.mkv",
    # Indian Reality
    "Bigg.Boss.S17E01.1080p.WEB-DL.mkv",
    "Kaun.Banega.Crorepati.S15E01.1080p.WEB-DL.mkv",
    "Khatron.Ke.Khiladi.S14E01.1080p.WEB-DL.mkv",
    "Dance.India.Dance.S08E01.720p.WEB-DL.mkv",
    "MTV.Roadies.S19E01.720p.WEB-DL.mkv",
]

# ============================================================================
# TALK SHOWS — Drop_Shows (25 files)
# ============================================================================
talk_shows = [
    "The.Tonight.Show.Starring.Jimmy.Fallon.S12E01.720p.WEB-DL.mkv",
    "The.Late.Show.with.Stephen.Colbert.S09E01.720p.WEB-DL.mkv",
    "Jimmy.Kimmel.Live.2024.01.15.1080p.WEB-DL.mkv",
    "Last.Week.Tonight.with.John.Oliver.S11E01.1080p.WEB-DL.mkv",
    "The.Daily.Show.2024.01.08.1080p.WEB-DL.mkv",
    "Conan.OBrien.Must.Go.S01E01.1080p.WEB-DL.mkv",
    "The.Graham.Norton.Show.S34E01.720p.HDTV.mkv",
    "Late.Night.with.Seth.Meyers.S11E01.720p.WEB-DL.mkv",
    "Real.Time.with.Bill.Maher.S22E01.1080p.WEB-DL.mkv",
    "The.Drew.Barrymore.Show.S04E01.720p.WEB-DL.mkv",
    "The.Jennifer.Hudson.Show.S03E01.720p.WEB-DL.mkv",
    "Hot.Ones.S23E01.1080p.WEB-DL.mkv",
    "Koffee.with.Karan.S08E01.1080p.WEB-DL.mkv",
    "The.Kapil.Sharma.Show.S04E01.1080p.WEB-DL.mkv",
    "Aap.Ki.Adalat.S01E01.720p.WEB-DL.mkv",
    "The.Great.Indian.Kapil.Show.S01E01.1080p.NF.WEB-DL.mkv",
    "Chatshow.With.Ravi.S02E01.720p.WEB-DL.mkv",
    "Nandini.Days.S01E01.720p.WEB-DL.mkv",
    "The.View.S28E01.720p.WEB-DL.mkv",
    "Watch.What.Happens.Live.S21E01.720p.WEB-DL.mkv",
    "The.Kelly.Clarkson.Show.S06E01.720p.WEB-DL.mkv",
    "Desus.and.Mero.S04E01.1080p.WEB-DL.mkv",
    "Parkinson.S01E01.720p.WEB-DL.mkv",
    "The.Jonathan.Ross.Show.S20E01.720p.HDTV.mkv",
    "Skavlan.S25E01.720p.WEB-DL.mkv",
]

# ============================================================================
# DOCUMENTARIES — Drop_Shows (series) + Drop_Movies (films)
# ============================================================================
doc_series = [
    # Nature
    "Planet.Earth.III.S01E01.1080p.BluRay.mkv",
    "Blue.Planet.II.S01E01.1080p.BluRay.mkv",
    "Our.Planet.S02E01.1080p.NF.WEB-DL.mkv",
    "Frozen.Planet.II.S01E01.1080p.BluRay.mkv",
    "A.Perfect.Planet.S01E01.1080p.BluRay.mkv",
    # True Crime
    "Making.a.Murderer.S02E01.1080p.NF.WEB-DL.mkv",
    "Tiger.King.S02E01.1080p.NF.WEB-DL.mkv",
    "The.Jinx.S02E01.1080p.WEB-DL.mkv",
    "Wild.Wild.Country.S01E01.1080p.NF.WEB-DL.mkv",
    "Murdaugh.Murders.A.Southern.Scandal.S02E01.1080p.NF.WEB-DL.mkv",
    # History
    "The.World.at.War.S01E01.1080p.BluRay.mkv",
    "The.Civil.War.S01E01.REMASTERED.1080p.BluRay.mkv",
    "Cosmos.A.Spacetime.Odyssey.S01E01.1080p.BluRay.mkv",
    "Chernobyl.S01E01.1080p.WEB-DL.mkv",
    "Band.of.Brothers.S01E01.REMASTERED.1080p.BluRay.mkv",
    # Social / Political
    "The.Last.Dance.S01E01.1080p.NF.WEB-DL.mkv",
    "Formula.1.Drive.to.Survive.S06E01.1080p.NF.WEB-DL.mkv",
    "Welcome.to.Wrexham.S03E01.1080p.WEB-DL.mkv",
    "The.Toys.That.Made.Us.S03E01.1080p.NF.WEB-DL.mkv",
    "Chef.s.Table.S07E01.1080p.NF.WEB-DL.mkv",
    # Indian
    "Scam.2003.The.Telgi.Story.S01E01.1080p.WEB-DL.mkv",
    "Indian.Predator.S03E01.1080p.NF.WEB-DL.mkv",
    "Crime.Stories.India.Detectives.S01E01.1080p.NF.WEB-DL.mkv",
    "Bad.Boy.Billionaires.India.S01E01.1080p.NF.WEB-DL.mkv",
    "House.of.Secrets.The.Burari.Deaths.S01E01.1080p.NF.WEB-DL.mkv",
    # Tech / Science
    "The.Social.Dilemma.S01E01.1080p.NF.WEB-DL.mkv",
    "Abstract.The.Art.of.Design.S02E06.1080p.NF.WEB-DL.mkv",
    "High.Score.S01E06.1080p.NF.WEB-DL.mkv",
    "Connected.S01E06.1080p.NF.WEB-DL.mkv",
    "Down.to.Earth.with.Zac.Efron.S02E08.1080p.NF.WEB-DL.mkv",
]

doc_movies = [
    "Free.Solo.2018.1080p.BluRay.x264.mkv",
    "Won't.You.Be.My.Neighbor.2018.1080p.BluRay.mkv",
    "The.Act.of.Killing.2012.DIRECTORS.CUT.1080p.BluRay.mkv",
    "Bowling.for.Columbine.2002.1080p.BluRay.mkv",
    "An.Inconvenient.Truth.2006.1080p.BluRay.mkv",
    "Blackfish.2013.1080p.BluRay.mkv",
    "Searching.for.Sugar.Man.2012.1080p.BluRay.mkv",
    "Amy.2015.1080p.BluRay.mkv",
    "RBG.2018.1080p.BluRay.mkv",
    "The.Social.Dilemma.2020.1080p.NF.WEB-DL.mkv",
    "Seaspiracy.2021.1080p.NF.WEB-DL.mkv",
    "My.Octopus.Teacher.2020.1080p.NF.WEB-DL.mkv",
    "Fire.of.Love.2022.1080p.WEB-DL.mkv",
    "Navalny.2022.1080p.WEB-DL.mkv",
    "All.the.Beauty.and.the.Bloodshed.2022.1080p.WEB-DL.mkv",
    "20.Days.in.Mariupol.2023.1080p.WEB-DL.mkv",
    "Beyond.Utopia.2023.1080p.WEB-DL.mkv",
    "Still.A.Revolution.2023.1080p.WEB-DL.mkv",
    "Daughters.2024.1080p.NF.WEB-DL.mkv",
    "Super.Size.Me.2004.1080p.WEB-DL.mkv",
]

# ============================================================================
# STAND-UP COMEDY — Drop_Movies (30 files)
# ============================================================================
standup = [
    # US Legends
    "Dave.Chappelle.The.Dreamer.2023.1080p.NF.WEB-DL.mkv",
    "Dave.Chappelle.The.Closer.2021.1080p.NF.WEB-DL.mkv",
    "Bo.Burnham.Inside.2021.1080p.NF.WEB-DL.mkv",
    "John.Mulaney.Baby.J.2023.1080p.NF.WEB-DL.mkv",
    "Chris.Rock.Selective.Outrage.2023.1080p.NF.WEB-DL.mkv",
    "Kevin.Hart.Reality.Check.2023.1080p.WEB-DL.mkv",
    "Trevor.Noah.Where.Was.I.2023.1080p.NF.WEB-DL.mkv",
    "Ali.Wong.Single.Lady.2024.1080p.NF.WEB-DL.mkv",
    "Hasan.Minhaj.The.Kings.Jester.2022.1080p.NF.WEB-DL.mkv",
    "Taylor.Tomlinson.Have.It.All.2024.1080p.NF.WEB-DL.mkv",
    "Matt.Rife.Natural.Selection.2023.1080p.NF.WEB-DL.mkv",
    "Nate.Bargatze.The.Greatest.Average.American.2021.1080p.NF.WEB-DL.mkv",
    "Sebastian.Maniscalco.Is.It.Me.2022.1080p.NF.WEB-DL.mkv",
    "Bill.Burr.Live.at.Red.Rocks.2022.1080p.NF.WEB-DL.mkv",
    "Tom.Segura.Sledgehammer.2023.1080p.NF.WEB-DL.mkv",
    "Ricky.Gervais.Armageddon.2023.1080p.NF.WEB-DL.mkv",
    "Gabriel.Iglesias.Stadium.Fluffy.2022.1080p.NF.WEB-DL.mkv",
    "Jim.Gaffigan.Dark.Pale.2024.1080p.AMZN.WEB-DL.mkv",
    "Bert.Kreischer.Razzle.Dazzle.2023.1080p.NF.WEB-DL.mkv",
    "Neal.Brennan.Crazy.Good.2024.1080p.NF.WEB-DL.mkv",
    # Indian Stand-Up
    "Vir.Das.Landing.2022.1080p.NF.WEB-DL.mkv",
    "Biswa.Kalyan.Rath.Biswa.Mast.Aadmi.2017.1080p.AMZN.WEB-DL.mkv",
    "Kenny.Sebastian.The.Most.Interesting.Person.in.the.Room.2020.1080p.NF.WEB-DL.mkv",
    "Abhishek.Upmanyu.Thoda.Saaf.Bol.2023.1080p.WEB-DL.mkv",
    "Zakir.Khan.Kaksha.Gyarvi.2018.1080p.AMZN.WEB-DL.mkv",
    # UK / International
    "James.Acaster.Cold.Lasagne.Hate.Myself.1999.2024.1080p.WEB-DL.mkv",
    "Jimmy.Carr.Natural.Born.Killer.2024.1080p.NF.WEB-DL.mkv",
    "Hannah.Gadsby.Something.Special.2023.1080p.NF.WEB-DL.mkv",
    "Russell.Howard.Lubricant.2024.1080p.NF.WEB-DL.mkv",
    "Wil.Anderson.Wiluminium.2023.1080p.WEB-DL.mkv",
]

# ============================================================================
# EDGE CASES — Mixed difficulty scenarios
# ============================================================================
edge_cases_shows = [
    # Multi-episode files
    "The.Office.US.S02E01E02.1080p.BluRay.mkv",
    # Season packs (folder simulation)
    "Fargo.S05E10.FINALE.1080p.WEB-DL.mkv",
    # Version tags
    "Naruto.S01E01.v2.1080p.BluRay.mkv",
    # Extremely long filename
    "The.Real.Housewives.of.Beverly.Hills.S13E01.Lets.Not.Be.Petty.1080p.WEB-DL.mkv",
    # Dots in show name
    "Mr.Robot.S04E13.1080p.WEB-DL.mkv",
    "S.W.A.T.S07E13.1080p.WEB-DL.mkv",
    "Marvel.s.Agents.of.S.H.I.E.L.D.S07E13.1080p.WEB-DL.mkv",
    # Year in show name
    "9-1-1.S07E13.1080p.WEB-DL.mkv",
    "1883.S01E10.1080p.WEB-DL.mkv",
    "1923.S02E08.1080p.WEB-DL.mkv",
    # Non-English show names
    "Aarya.S03E08.1080p.WEB-DL.mkv",
    "Maharani.S03E10.1080p.WEB-DL.mkv",
    # Absurdly short name
    "24.S09E12.1080p.WEB-DL.mkv",
    "9-1-1.Lone.Star.S04E18.1080p.WEB-DL.mkv",
    "The.100.S07E16.1080p.WEB-DL.mkv",
]

edge_cases_movies = [
    # Single character/number titles
    "M.1931.REMASTERED.1080p.BluRay.mkv",
    "Z.1969.Criterion.1080p.BluRay.mkv",
    "9.2009.1080p.BluRay.mkv",
    # Colons in title (encoded as dots)
    "Star.Wars.Episode.IV.A.New.Hope.1977.1080p.BluRay.mkv",
    "Indiana.Jones.and.the.Dial.of.Destiny.2023.1080p.WEB-DL.mkv",
    # Very long titles
    "Borat.Subsequent.Moviefilm.Delivery.of.Prodigious.Bribe.to.American.Regime.2020.1080p.WEB-DL.mkv",
    "Dr.Strangelove.or.How.I.Learned.to.Stop.Worrying.and.Love.the.Bomb.1964.1080p.BluRay.mkv",
    # Remakes / same title different year
    "Suspiria.2018.1080p.BluRay.mkv",
    "Dune.1984.1080p.BluRay.mkv",
    "The.Thing.2011.1080p.BluRay.mkv",
    # Numbers that look like years
    "2012.2009.1080p.BluRay.mkv",
    "300.Rise.of.an.Empire.2014.1080p.BluRay.mkv",
    "1408.2007.DIRECTORS.CUT.1080p.BluRay.mkv",
    "10.Things.I.Hate.About.You.1999.1080p.BluRay.mkv",
    "28.Days.Later.2002.1080p.BluRay.mkv",
]

# ============================================================================
# FOLDER STRUCTURE SCENARIOS — Real-world drops (folders, not flat files)
# ============================================================================

def mkfolder(*parts):
    """Create a nested folder and return its Path."""
    p = Path(*parts)
    p.mkdir(parents=True, exist_ok=True)
    return p

def touch_in(folder, name):
    """Create a dummy file inside a folder."""
    (Path(folder) / name).write_bytes(b'\x00' * 5)

folder_file_count = 0

def create_folder_scenarios():
    global folder_file_count

    # --- 1. Haikyuu Complete Series (Drop_Shows) ---
    # Tests: CTX sidecar, OAD/OVA specials, no-SxxEyy files, season subtitle
    root = mkfolder(DROP_SHOWS, "Haikyuu!!")
    for s, eps in [(1, 3), (2, 3), (3, 2)]:
        sd = mkfolder(root, f"Season {s}")
        for e in range(1, eps + 1):
            touch_in(sd, f"Haikyuu!! - {e:02d}.mkv")
            folder_file_count += 1
    # Season 4 — files already have SxxEyy
    s4 = mkfolder(root, "Season 4")
    for e in range(1, 3):
        touch_in(s4, f"Haikyuu!! S04E{e:02d}.mkv")
        folder_file_count += 1
    # OAD and OVA folders
    oad = mkfolder(root, "OAD")
    touch_in(oad, "Haikyuu!! OAD 01.mkv")
    touch_in(oad, "Haikyuu!! OAD 02.mkv")
    folder_file_count += 2
    ova = mkfolder(root, "OVA")
    touch_in(ova, "Haikyuu!! OVA 01.mkv")
    touch_in(ova, "Haikyuu!! OVA 02.mkv")
    folder_file_count += 2

    # --- 2. Silicon Valley Complete (Drop_Shows) ---
    # Tests: nested folder with SxxEyy files, complete series in one folder
    sv = mkfolder(DROP_SHOWS, "Silicon.Valley.Complete.S01-S06.1080p.BluRay")
    for s in range(1, 7):
        touch_in(sv, f"Silicon.Valley.S{s:02d}E01.1080p.BluRay.mkv")
        touch_in(sv, f"Silicon.Valley.S{s:02d}E02.1080p.BluRay.mkv")
        folder_file_count += 2

    # --- 3. Ted Lasso Season Packs (Drop_Shows) ---
    # Tests: multiple folders, junk file filtering
    for s, eps in [(1, 3), (2, 3), (3, 2)]:
        tl = mkfolder(DROP_SHOWS, f"Ted.Lasso.S{s:02d}.1080p.ATVP.WEB-DL.DDP5.1")
        for e in range(1, eps + 1):
            touch_in(tl, f"Ted.Lasso.S{s:02d}E{e:02d}.1080p.mkv")
            folder_file_count += 1
    # Junk files in S03 folder
    s3_folder = DROP_SHOWS / "Ted.Lasso.S03.1080p.ATVP.WEB-DL.DDP5.1"
    touch_in(s3_folder, "readme.txt")
    touch_in(s3_folder, "sample.jpg")

    # --- 4. Captain America Trilogy (Drop_Movies) ---
    # Tests: movie folders with junk (nfo, srt, sample)
    ca1 = mkfolder(DROP_MOVIES, "Captain.America.The.First.Avenger.2011.1080p.BluRay.x264")
    touch_in(ca1, "Captain.America.The.First.Avenger.2011.1080p.BluRay.x264.mkv")
    touch_in(ca1, "Captain.America.The.First.Avenger.nfo")
    touch_in(ca1, "Captain.America.The.First.Avenger.srt")
    folder_file_count += 1
    ca2 = mkfolder(DROP_MOVIES, "Captain.America.The.Winter.Soldier.2014.1080p.BluRay.x264")
    touch_in(ca2, "Captain.America.The.Winter.Soldier.2014.1080p.BluRay.x264.mkv")
    subs = mkfolder(ca2, "Subs")
    touch_in(subs, "English.srt")
    folder_file_count += 1
    ca3 = mkfolder(DROP_MOVIES, "Captain.America.Civil.War.2016.1080p.BluRay.x264")
    touch_in(ca3, "Captain.America.Civil.War.2016.1080p.BluRay.x264.mkv")
    touch_in(ca3, "sample.mkv")
    folder_file_count += 2  # both mkv files get promoted

    # --- 5. Iron Man Trilogy (Drop_Movies) ---
    # Tests: year-in-folder-name format
    for title, year in [("Iron Man", 2008), ("Iron Man 2", 2010), ("Iron Man 3", 2013)]:
        im = mkfolder(DROP_MOVIES, f"{title} ({year})")
        safe_title = title.replace(" ", ".")
        touch_in(im, f"{safe_title}.{year}.1080p.BluRay.mkv")
        folder_file_count += 1

    # --- 6. Jujutsu Kaisen Mixed (Drop_Shows + Drop_Movies) ---
    # Tests: bracket-tagged anime folders, featurettes subfolder, movie
    jjk1 = mkfolder(DROP_SHOWS, "[SubsPlease] Jujutsu Kaisen S01 [1080p]")
    for e in range(1, 4):
        touch_in(jjk1, f"[SubsPlease] Jujutsu Kaisen - {e:02d} [1080p].mkv")
        folder_file_count += 1
    feat = mkfolder(jjk1, "Featurettes")
    touch_in(feat, "Behind.the.Scenes.mkv")
    folder_file_count += 1  # featurette video also gets promoted

    jjk2 = mkfolder(DROP_SHOWS, "[SubsPlease] Jujutsu Kaisen Season 2 [1080p] [WEB-DL]")
    for e in range(1, 4):
        touch_in(jjk2, f"[SubsPlease] Jujutsu Kaisen S2 - {e:02d} [1080p].mkv")
        folder_file_count += 1

    # JJK movie → Drop_Movies
    jjk0 = mkfolder(DROP_MOVIES, "Jujutsu.Kaisen.0.2022.1080p.BluRay.x264")
    touch_in(jjk0, "Jujutsu.Kaisen.0.2022.1080p.BluRay.x264.mkv")
    jjk0_subs = mkfolder(jjk0, "subs")
    touch_in(jjk0_subs, "eng.srt")
    folder_file_count += 1

    # --- 7. Kung Fu Panda KatmovieHD (Drop_Shows) ---
    # Tests: scene-tagged filenames with site watermark
    kfp = mkfolder(DROP_SHOWS, "Kung.Fu.Panda.Legends.of.Awesomeness.S01.720p.WEB-DL.KatmovieHD")
    for e in range(1, 4):
        touch_in(kfp, f"KatmovieHD.com - Kung.Fu.Panda.Legends.of.Awesomeness.S01E{e:02d}.720p.mkv")
        folder_file_count += 1

    # --- 8. Attack on Titan Specials (Drop_Shows) ---
    # Tests: OVA/Specials with anime shows
    aot = mkfolder(DROP_SHOWS, "Attack on Titan")
    aot_s1 = mkfolder(aot, "Season 1")
    touch_in(aot_s1, "Attack.on.Titan.S01E01.mkv")
    touch_in(aot_s1, "Attack.on.Titan.S01E02.mkv")
    folder_file_count += 2
    aot_sp = mkfolder(aot, "Specials")
    touch_in(aot_sp, "Attack on Titan Special 1.mkv")
    touch_in(aot_sp, "Attack on Titan Special 2.mkv")
    folder_file_count += 2
    aot_ova = mkfolder(aot, "OVA")
    touch_in(aot_ova, "Attack on Titan OVA 1.mkv")
    folder_file_count += 1

    # --- 9. Documentary Series Folder (Drop_Shows) ---
    # Tests: documentary classification with folder structure
    pe = mkfolder(DROP_SHOWS, "Planet.Earth.III.S01.1080p.BluRay")
    for e, title in enumerate(["Coasts", "Ocean", "Freshwater"], 1):
        touch_in(pe, f"Planet.Earth.III.S01E{e:02d}.{title}.1080p.BluRay.mkv")
        folder_file_count += 1

    # --- 10. Stand-Up Special Folder (Drop_Movies) ---
    # Tests: stand-up detection with folder + junk image
    dc = mkfolder(DROP_MOVIES, "Dave.Chappelle.The.Dreamer.2023.1080p.NF.WEB-DL")
    touch_in(dc, "Dave.Chappelle.The.Dreamer.2023.1080p.NF.WEB-DL.mkv")
    touch_in(dc, "Dave.Chappelle.The.Dreamer.2023.jpg")
    folder_file_count += 1

    print(f"\n  Folder Scenarios Created: {folder_file_count} video files in folders")

# ============================================================================
# CREATE ALL FILES
# ============================================================================

counts = {}

def create_batch(folder, files, label):
    for f in files:
        touch(folder, f)
    counts[label] = len(files)

# Movies → Drop_Movies
create_batch(DROP_MOVIES, hollywood, "Hollywood Movies")
create_batch(DROP_MOVIES, bollywood, "Bollywood Movies")
create_batch(DROP_MOVIES, tollywood, "South Indian Movies")
create_batch(DROP_MOVIES, international, "International Movies")
create_batch(DROP_MOVIES, anime_movies, "Anime Movies")
create_batch(DROP_MOVIES, doc_movies, "Documentary Films")
create_batch(DROP_MOVIES, standup, "Stand-Up Comedy")
create_batch(DROP_MOVIES, edge_cases_movies, "Edge Case Movies")

# Shows → Drop_Shows
create_batch(DROP_SHOWS, indian_tv, "Indian TV Shows")
create_batch(DROP_SHOWS, foreign_tv, "Foreign TV Shows")
create_batch(DROP_SHOWS, cartoons_indian, "Indian Cartoons")
create_batch(DROP_SHOWS, cartoons_international, "Int'l Cartoons")
create_batch(DROP_SHOWS, anime_series, "Anime Series")
create_batch(DROP_SHOWS, reality_tv, "Reality TV")
create_batch(DROP_SHOWS, talk_shows, "Talk Shows")
create_batch(DROP_SHOWS, doc_series, "Documentary Series")
create_batch(DROP_SHOWS, edge_cases_shows, "Edge Case Shows")

# Folder Structures
create_folder_scenarios()

# Summary
print("=" * 60)
print("  STRESS TEST FILES CREATED")
print("=" * 60)
total_movies = 0
total_shows = 0
for label, count in counts.items():
    bucket = "Drop_Movies" if "Movie" in label or "Stand" in label or "Film" in label else "Drop_Shows"
    if bucket == "Drop_Movies":
        total_movies += count
    else:
        total_shows += count
    print(f"  {label:.<40} {count:>4} -> {bucket}")

print("-" * 60)
print(f"  {'Drop_Movies TOTAL (flat)':.<40} {total_movies:>4}")
print(f"  {'Drop_Shows TOTAL (flat)':.<40} {total_shows:>4}")
print(f"  {'Folder Scenarios (videos)':.<40} {folder_file_count:>4}")
print(f"  {'GRAND TOTAL':.<40} {total_movies + total_shows + folder_file_count:>4}")
print("=" * 60)
print("\nFiles are ready in Drop_Shows/ and Drop_Movies/")
print("Start services from the web panel to begin processing.")
