import os
from scraper import scraper
from config import scraper_flats_url, scraper_flats_deep, scraper_flats_sort, scraper_flats_result_dir
import logging
from CArgparse import CArgparse
import sys
from apscheduler.schedulers.background import BackgroundScheduler
from time import sleep

logging.basicConfig(level=logging.INFO, filemode="a",
                    format="%(asctime)s %(levelname)s %(message)s")

if not os.path.isdir(scraper_flats_result_dir):
    os.mkdir(scraper_flats_result_dir)
if not os.path.isdir(f"{scraper_flats_result_dir}/raw_json"):
    os.mkdir(f"{scraper_flats_result_dir}/raw_json")




def main():
    cargparse = CArgparse()
    argsdict = cargparse.argsAsDict()
    deep = scraper_flats_deep
    deep = argsdict.get("deep", deep)
    sort = scraper_flats_sort
    sort = argsdict.get("sort", sort)

    match argsdict["scenario"]:
        case "once":
            scraper(scraper_flats_result_dir, scraper_flats_url, deep, sort, "flats")
        case "cron":
            scheduler = BackgroundScheduler(timezone="Asia/Yekaterinburg")

            scheduler.add_job(scraper, 'cron', hour=8, minute=7,
                              args=[scraper_flats_result_dir, scraper_flats_url, deep, sort, "flats"])

            scheduler.start()

            while True:
                sleep(1)



if __name__ == "__main__":
    main()
