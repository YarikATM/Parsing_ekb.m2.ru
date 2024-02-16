import os
from scraper import scraper
from config import scraper_rooms_sort, scraper_rooms_deep, scraper_rooms_result_dir, scraper_rooms_url
import logging
from CArgparse import CArgparse
import sys
from apscheduler.schedulers.background import BackgroundScheduler
from time import sleep

logging.basicConfig(level=logging.INFO, filemode="a",
                    format="%(asctime)s %(levelname)s %(message)s")

if not os.path.isdir(scraper_rooms_result_dir):
    os.mkdir(scraper_rooms_result_dir)
if not os.path.isdir(f"{scraper_rooms_result_dir}/raw_json"):
    os.mkdir(f"{scraper_rooms_result_dir}/raw_json")




def main():
    cargparse = CArgparse()
    argsdict = cargparse.argsAsDict()
    deep = scraper_rooms_deep
    deep = argsdict.get("deep", deep)
    sort = scraper_rooms_sort
    sort = argsdict.get("sort", sort)

    match argsdict["scenario"]:
        case "once":
            scraper(scraper_rooms_result_dir, scraper_rooms_url, deep, sort, "rooms")
        case "cron":
            scheduler = BackgroundScheduler(timezone="Asia/Yekaterinburg")

            scheduler.add_job(scraper, 'cron', hour=8, minute=7,
                              args=[scraper_flats_result_dir, scraper_flats_url, deep, sort, "rooms"])

            scheduler.start()

            while True:
                sleep(1)




if __name__ == "__main__":
    main()
