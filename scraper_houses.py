import os
from scraper import scraper
from config import scraper_houses_sort, scraper_houses_result_dir, scraper_houses_deep, scraper_houses_url
import logging
from CArgparse import CArgparse
import sys
from apscheduler.schedulers.background import BackgroundScheduler
from time import sleep

logging.basicConfig(level=logging.INFO, filemode="a",
                    format="%(asctime)s %(levelname)s %(message)s")

if not os.path.isdir(scraper_houses_result_dir):
    os.mkdir(scraper_houses_result_dir)
if not os.path.isdir(f"{scraper_houses_result_dir}/raw_json"):
    os.mkdir(f"{scraper_houses_result_dir}/raw_json")




def main():
    cargparse = CArgparse()
    argsdict = cargparse.argsAsDict()
    deep = scraper_houses_deep
    deep = argsdict.get("deep", deep)
    sort = scraper_houses_sort
    sort = argsdict.get("sort", sort)


    match argsdict["scenario"]:
        case "once":
            scraper(scraper_houses_result_dir, scraper_houses_url, deep, sort, "houses")
        case "cron":
            scheduler = BackgroundScheduler(timezone="Asia/Yekaterinburg")

            scheduler.add_job(scraper, 'cron', hour=8, minute=7,
                              args=[scraper_houses_result_dir, scraper_houses_url, deep, sort, "houses"])

            scheduler.start()

            while True:
                sleep(1)



if __name__ == "__main__":
    main()
