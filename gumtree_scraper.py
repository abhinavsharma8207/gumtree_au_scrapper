import re
from concurrent.futures import as_completed
import requests
from bs4 import BeautifulSoup
from requests_futures.sessions import FuturesSession
import json
from database import *
from utilities import *
import datetime
from datetime import datetime as dt
import logger
import sys


PROXY_HOST = ''  # rotating proxy or host
PROXY_PORT = ''  # port
PROXY_USER = ''  # username
PROXY_PASS = ''  # password
time_interval_arr = ["seconds", "second", "hours", "hour", "minutes", "minute"]


class GumtreeScraper:

    def __init__(self):
        url = "http://{}:{}@{}:{}".format(PROXY_USER, PROXY_PASS, PROXY_HOST, PROXY_PORT)
        proxies = {'http': url}
        self.proxy = proxies
        self.urls = ["https://www.gumtree.com.au/s-cars-vans-utes/c18320?ad=offering"]
        self.headers = {
            'authority': 'www.gumtree.com.au',
            'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36',
            'sec-ch-ua-platform': '"macOS"',
            'accept': '*/*',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.gumtree.com.au/s-ad/south-melbourne/cars-vans-utes/2010-porsche-cayenne-92a-my11-s-tiptronic-white-8-speed-sports-automatic-wagon/1276574250',
            'accept-language': 'en-US,en;q=0.9',
            'cookie': 'machId=Axdqd060ZQKdoli9HsbbLIeE4ELmbdvakQpvUPxtgA5wVEoMm6CjVVjHGKfDdZO0Q0Z1my5zMgJJXRvH8diyVo44VUl6V3KXiqo; bs=%7B%22st%22%3A%7B%7D%7D; ftv=true; __utmc=160852194; _ga=GA1.3.525013564.1632400829; _pbjs_userid_consent_data=3524755945110770; optimizelyEndUserId=oeu1632400829272r0.4117340747657485; warningToUser=true; _fbp=fb.2.1632400829573.214851794; AMCVS_50BE5F5858D2477A0A495C7F%40AdobeOrg=1; s_ecid=MCMID%7C08904635077464500782721281415874895018; __gads=ID=dd88fa079e9a9af3:T=1632400830:S=ALNI_Ma3IdR6qQTbqzhaaLmjevTA8ufxYQ; aam_tnt=aamsegid%3D6797281%2Caamsegid%3D6880889; aam_uuid=09289786456739853802701040440113034064; libtg=a; ki_s=219083%3A0.0.0.0.0; rbzsessionid=c8718e55139ca8cc1d7b6898065865b9; __utmz=160852194.1632648785.4.4.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); ki_r=; rbzid=Jz8ldo3xSsBqmpJ0ag5WZJdWyHs7uSzuOQS6UIceq4F6idyCKon11897vsp/KP20OeaojnWChlbn6g4dxWZ+6hJxsCAy7Z5VrUTO+TZK2S2y7T/HmKIKjfxgpE+Iol9HJq03n0B1NLfNE6+5KLFQ/oubrt8IKGmATt+mCCYik5rhwhmk+wo9LeqruvmBdRCYG0vywpuLMRQGVvQC+cdn/WUXFldsA4xn1d2NMCZ3/uT63Yl/+/6KbbiJX1eiTSmB47a5+FdmI+QcgPzI1FE8tBy91H97sazqHv7GLoFJpwCQ/wn7ZdsWgdrYIlVRjuH7TpBPYi7SnyydzMvnGJrBPQ==; __utma=160852194.525013564.1632400829.1632648785.1632831616.5; _gid=GA1.3.1115036861.1633244675; JSESSIONID=E6BC5A28F7BF40A14FB4FAB3F45EDA6F; AMCV_50BE5F5858D2477A0A495C7F%40AdobeOrg=-408604571%7CMCIDTS%7C18904%7CMCMID%7C08904635077464500782721281415874895018%7CMCAAMLH-1633524722%7C6%7CMCAAMB-1633244674%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1633251875s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C4.6.0; up=%7B%22nps%22%3A%2224%22%2C%22lbh%22%3A%22l%3D0%26c%3D18320%26r%3D0%26sv%3DLIST%26at%3DOFFER%26sf%3Ddate%26icr%3Dfalse%22%2C%22rva%22%3A%221275675503%2C1281649033%2C1272792488%2C1269855817%2C1282094788%22%2C%22lsh%22%3A%22l%3D0%26c%3D18320%26r%3D0%26sv%3DLIST%26at%3DOFFER%26sf%3Ddate%26icr%3Dfalse%22%2C%22ls%22%3A%22l%3D0%26r%3D10%26sv%3DLIST%26sf%3Ddate%22%7D; cto_bundle=KPtWRF9BS2Zac0FrbUxLYzh6THV5SU9oYWw3TCUyRm9ZOW55TWp6T0ElMkZxdzY3SERyUFQlMkJobFYya1dweG1BJTJCblNXV2t6SHJBcHA5b21zQ2VrV1lobmFIem8wWGo1YnJLUlpKQyUyRjViaTdjN0xCN0tuRU9nJTJGNGM2eDhwakdROFBSbEZlWVV0TkUlMkY1eWJiN3NzcGlwZUlKdWpmbU16VjBNY1Q2T3dmV0xDVml1NTRlT1BkR1h2REJnWlNiYWNZSjZvcXBaa0kwRnVSdElOOWMzZEswRHB2Vk16R092S2clM0QlM0Q; ki_t=1632400830815%3B1633244677381%3B1633244677381%3B7%3B68; aam_dfp=aamsegid%3D6797281%2C6880889%2C7087286%2C7220740%2C7329284%2C7329285%2C7329301%2C7333796%2C8210148%2C8269554%2C8330575%2C8444126%2C8448751%2C8448793%2C8465423%2C8487238%2C8975234%2C8978952%2C8978953%2C8978957%2C8978963%2C8978964%2C9131852%2C8458219%2C9501649%2C21229478%2C21229714%2C13639936%2C22019790%2C22019860; _lr_retry_request=true',
        }

    def get_gumtree_listings(self):
        try:
            recent_gumtree_ids = []
            cars_listings = []
            insertion_listings = []
            self.populate_list_of_url_pages()
            lst_splited = chunks(self.urls, 5)
            for lst in lst_splited:
                with FuturesSession() as session:
                    futures = [session.get(url, proxies=self.proxy) for url
                               in lst]
                    for future in as_completed(futures):
                        if future.result().status_code == 200:
                            soup = BeautifulSoup(future.result().content, 'html5lib')
                            all_ads = soup.findAll('div',
                                                   attrs={'class': 'user-ad-collection-new-design__wrapper--row'})
                            for source in all_ads:
                                adds_list = source.contents
                                for add in adds_list:
                                    attrs = add.attrs
                                    if "href" in attrs and "class" in attrs:
                                        add_category = attrs["class"]
                                        for category in add_category:
                                            if category == "user-ad-row-new-design--cars-category":
                                                description = add.findAll('p', attrs={
                                                    'class': 'user-ad-row-new-design__description-text'})
                                                car_listing = dict()
                                                car_listing["description"] = str(description[0].contents[0].string)
                                                car_listing["gumtree_id"] = attrs["id"].split("-")[2]
                                                recent_gumtree_ids.append(car_listing["gumtree_id"])
                                                splitAdDetails = attrs["aria-label"].splitlines()
                                                car_listing["title"] = splitAdDetails[0]
                                                car_listing["location"] = splitAdDetails[2].split(".")[0].replace(
                                                    "Location:", "").replace(".", "").strip()
                                                date_listed_arr = splitAdDetails[2].split(".")[1].replace("Ad listed",
                                                                                                          "").strip().split(
                                                    "/")
                                                if len(date_listed_arr) > 1:
                                                    date_listed = date_listed_arr[2] + "-" + date_listed_arr[1] + "-" + \
                                                                  date_listed_arr[0]
                                                    date_listed = date_listed + " " + str(
                                                        dt.now().time().replace(microsecond=0))
                                                    created_on = dt.strptime(date_listed, '%Y-%m-%d %H:%M:%S')
                                                else:
                                                    for time_interval in time_interval_arr:
                                                        if time_interval in date_listed_arr[0].lower():
                                                            created_on = dt.now().replace(microsecond=0)
                                                    if "yesterday" in date_listed_arr[0].lower():
                                                        today = datetime.date.today()
                                                        yesterday = today - datetime.timedelta(days=1)
                                                        created_on = yesterday
                                                car_listing["created_on"] = created_on
                                                car_listing["link"] = "https://www.gumtree.com.au{}".format(
                                                    attrs['href'])
                                                cars_listings.append(car_listing)

                    add_futures = []
                    for car_listing in cars_listings:
                        add_future = session.get(car_listing["link"], proxies=self.proxy, headers=self.headers)
                        add_future.car_listing = car_listing
                        add_futures.append(add_future)
                    for addFuture in as_completed(add_futures):
                        if addFuture.result().status_code == 200:
                            soup = BeautifulSoup(addFuture.result().content, 'html5lib')
                            script = soup.find_all('script')[9].string.strip()[40:-312]
                            try:
                                script = json.loads(script)
                            except Exception as e:
                                print("unable to parse json")
                                continue
                            addFuture.car_listing["phone"] = None
                            if 'highest_price' in script['a']['attr']:
                                addFuture.car_listing["price"] = script["a"]["attr"]["highest_price"]
                            else:
                                addFuture.car_listing["price"] = None
                            if 'carmake' in script['a']['attr']:
                                addFuture.car_listing["make"] = script["a"]["attr"]["carmake"]
                            else:
                                addFuture.car_listing["make"] = None
                            if 'carmodel' in script['a']['attr']:
                                addFuture.car_listing["model"] = script["a"]["attr"]["carmodel"].split("_")[1]
                            else:
                                addFuture.car_listing["model"] = None
                            if 'caryear' in script['a']['attr']:
                                addFuture.car_listing["year"] = script["a"]["attr"]["caryear"]
                            else:
                                addFuture.car_listing["year"] = None
                            if 'carmileageinkms' in script['a']['attr']:
                                addFuture.car_listing["kms"] = script["a"]["attr"]["carmileageinkms"]
                            else:
                                addFuture.car_listing["kms"] = None
                            if 'colour' in script['a']['attr']:
                                addFuture.car_listing["color"] = script["a"]["attr"]["colour"]
                            else:
                                addFuture.car_listing["color"] = None
                            addFuture.car_listing["doors"] = None
                            addFuture.car_listing["body_condition"] = None
                            addFuture.car_listing["mechanical_condition"] = None
                            addFuture.car_listing["specs"] = None
                            addFuture.car_listing["seller_type"] = script["a"]["attr"]["forsaleby"]
                            if 'variant' in script['a']['attr']:
                                addFuture.car_listing["variant"] = script["a"]["attr"]["variant"]
                            else:
                                addFuture.car_listing["variant"] = None
                            if 'carbodytype' in script['a']['attr']:
                                addFuture.car_listing["body_type"] = script["a"]["attr"]["carbodytype"]
                            else:
                                addFuture.car_listing["body_type"] = None
                            if 'cylinder_configuration' in script['a']['attr']:
                                addFuture.car_listing["cylinders"] = script["a"]["attr"][
                                    "cylinder_configuration"].replace(
                                    "cyl", "")
                            else:
                                addFuture.car_listing["cylinders"] = None
                            if 'cartransmission' in script['a']['attr']:
                                addFuture.car_listing["transmission"] = script["a"]["attr"]["cartransmission"]
                            else:
                                addFuture.car_listing["transmission"] = None
                            addFuture.car_listing["hp"] = None
                            if 'fueltype' in script['a']['attr']:
                                addFuture.car_listing["fuel_type"] = script["a"]["attr"]["fueltype"]
                            else:
                                addFuture.car_listing["fuel_type"] = None
                            addFuture.car_listing["warranty"] = None
                            addFuture.car_listing["extras"] = None
                            addFuture.car_listing["technical_features"] = None
                            if 'listing_source' in script['a']['attr']:
                                addFuture.car_listing["listed_by"] = script["a"]["attr"]["listing_source"]
                            else:
                                addFuture.car_listing["listed_by"] = None
                            if 'vin' in script['a']['attr']:
                                addFuture.car_listing["vin"] = script["a"]["attr"]["vin"]
                            else:
                                addFuture.car_listing["vin"] = None
                            if 'nvic' in script['a']['attr']:
                                addFuture.car_listing["nvic"] = script["a"]["attr"]["nvic"]
                                addFuture.car_listing["redbook_code"] = get_redbook_code(script["a"]["attr"]["nvic"])
                            else:
                                addFuture.car_listing["nvic"] = None
                                addFuture.car_listing["redbook_code"] = None
                            if 'ancap_rating' in script['a']['attr']:
                                addFuture.car_listing["ancap_rating"] = script["a"]["attr"]["ancap_rating"]
                            else:
                                addFuture.car_listing["ancap_rating"] = None
                            if 'l' in script:
                                if 'c' in script['l']:
                                    addFuture.car_listing["city"] = script['l']['c']['n']
                                else:
                                    addFuture.car_listing["city"] = None
                                if 'l1' in script['l']:
                                    addFuture.car_listing["state"] = script['l']['l1']['n']
                                else:
                                    addFuture.car_listing["state"] = None
                                if 'l0' in script['l']:
                                    addFuture.car_listing["country"] = script['l']['l0']['n']
                                else:
                                    addFuture.car_listing["country"] = None

                            insertion_listings.append(addFuture.car_listing)
                    sublists_of_listings = chunks(insertion_listings, 100)
                    for sublist in sublists_of_listings:
                        create_gumtree_listing(sublist)
            remove_gumtree_ids = get_gumtree_listings_by_gumtree_ids_for_removal(recent_gumtree_ids)
            update_gumtree_listing_for_removed_on(remove_gumtree_ids)
        except Exception as e:
            logger.log("Error in get_gumtree_listings: " + str(e))
            logger.log("Error on line {}".format(sys.exc_info()[-1].tb_lineno))

    def populate_list_of_url_pages(self):
        try:
            r = requests.get('https://www.gumtree.com.au/s-cars-vans-utes/c18320?ad=offering', proxies=self.proxy)
            soup = BeautifulSoup(r.content, 'html5lib')
            last_page_anchor = soup.findAll('a', attrs={
                'class': 'page-number-navigation__link page-number-navigation__link-last link link--base-color-primary link--hover-color-none link--no-underline'})
            last_page_url_string = last_page_anchor[0].attrs['href']
            pages = re.findall(r"page-(.*)", last_page_url_string)
            total_pages = int(pages[0].split("/")[0])
            x = range(2, total_pages)
            for n in x:
                self.urls.append("https://www.gumtree.com.au/s-automotive/page-{}/c9299r10".format(n))
        except Exception as e:
            logger.log("Error in populate_list_of_url_pages: " + str(e))
            logger.log("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
