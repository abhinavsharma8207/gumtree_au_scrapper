import csv
import sys

import mysql.connector
import pandas as pd
from datetime import datetime

import logger

DB_HOST_DS = ""
DB_USER_DS = ""
DB_PASS_DS = ""
DB_DATABASE_DS = ""

DB_HOST_SCP = ""
DB_DATABASE_SCP = ""
DB_USER_SCP = ""
DB_PASS_SCP = ""

DB_HOST_PRD = ""
DB_USER_PRD = ""
DB_PASS_PRD = ""
DB_DATABASE_PRD = ""


def get_redbook_code(nvic):
    glass_rb_df = pd.read_csv("GlassRB.csv")
    matches = glass_rb_df[glass_rb_df['NVIC'] == nvic]
    if len(matches) > 0:
        items = matches.drop('NVIC', axis=1).values.tolist()
        return items[0][0]
    else:
        return None


def download_redbook_codes():
    try:
        connection_ds = mysql.connector.connect(
            host=DB_HOST_PRD,
            user=DB_USER_PRD,
            password=DB_PASS_PRD,
            database=DB_DATABASE_PRD
        )
        cursor = connection_ds.cursor()
        query = "select RBCode, NVIC from GlassRB"
        cursor.execute(query)
        with open("GlassRB.csv", "w") as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(col[0] for col in cursor.description)
            for row in cursor:
                writer.writerow(row)
    except Exception as e:
        logger.log("Error in download_redbook_codes: " + str(e))
        logger.log("Error on line {}".format(sys.exc_info()[-1].tb_lineno))

    finally:
        if cursor is not None:
            cursor.close()
            connection_ds.close()


def get_gumtree_listings_by_gumtree_ids_for_removal(gumtree_ids):
    try:
        connection_ds = mysql.connector.connect(
            host=DB_HOST_DS,
            user=DB_USER_DS,
            password=DB_PASS_DS,
            database=DB_DATABASE_DS
        )
        cursor = connection_ds.cursor()
        format_strings = ','.join(['%s'] * len(gumtree_ids))
        cursor.execute(
            "Select gumtree_id from au_gumtree_listings WHERE gumtree_id not IN (%s) and removed_on is null" % format_strings,
            tuple(gumtree_ids))
        return [item[0] for item in cursor.fetchall()]
    except Exception as e:
        logger.log("Error in get_gumtree_listings_by_gumtree_ids: " + str(e))
        logger.log("Error on line {}".format(sys.exc_info()[-1].tb_lineno))

    finally:
        if cursor is not None:
            cursor.close()
            connection_ds.close()


def create_gumtree_listing(gumtree_listings):
    try:
        connection_ds = mysql.connector.connect(
            host=DB_HOST_DS,
            user=DB_USER_DS,
            password=DB_PASS_DS,
            database=DB_DATABASE_DS
        )
        cursor = connection_ds.cursor()
        val = []
        sql = "INSERT INTO au_gumtree_listings (gumtree_id, title, link, country, state," \
              "location, price, description, phone, make, model, year, kms, color, doors, body_condition, " \
              "mechanical_condition, specs, seller_type, body_type, cylinders, transmission, hp, fuel_type, vin, " \
              "nvic, ancap_rating, redbook_code, variant, listed_by, created_on) VALUES (%s, %s, %s, %s, %s, %s, " \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
              "%s) on duplicate key update title=values(title),link=values(link),country=values(country)," \
              "state=values(state),location=values(location),price=values(price),description=values(description)," \
              "phone=values(phone),make=values(make),model=values(model),year=values(year),kms=values(kms)," \
              "color=values(color),doors=values(doors),body_condition=values(body_condition)," \
              "mechanical_condition=values(mechanical_condition),specs=values(specs),seller_type=values(seller_type)," \
              "body_type=values(body_type),cylinders=values(cylinders),transmission=values(transmission),hp=values(" \
              "hp),fuel_type=values(fuel_type),vin=values(vin),nvic=values(nvic),ancap_rating=values(ancap_rating)," \
              "redbook_code=values(redbook_code),variant=values(variant),listed_by=values(listed_by),updated_on='{}'".format(
            str(datetime.now().replace(microsecond=0)))
        for listing in gumtree_listings:
            record = (listing["gumtree_id"], listing["title"], listing["link"], listing["country"], listing["state"],
                      listing["location"],
                      listing["price"], listing["description"], listing["phone"], listing["make"], listing["model"],
                      listing["year"], listing["kms"],
                      listing["color"], listing["doors"], listing["body_condition"], listing["mechanical_condition"],
                      listing["specs"],
                      listing["seller_type"], listing["body_type"], listing["cylinders"], listing["transmission"],
                      listing["hp"], listing["fuel_type"],
                      listing["vin"], listing["nvic"], listing["ancap_rating"], listing["redbook_code"],
                      listing["variant"], listing["listed_by"],
                      str(listing["created_on"]))
            val.append(record)
        cursor.executemany(sql, val)
        connection_ds.commit()
        logger.log(str(cursor.rowcount) + " was inserted or updated", "log")
        logger.log("create_gumtree_listings sql " + cursor.statement, "log")
    except Exception as e:
        logger.log("Error in create_gumtree_listing: " + str(e))
        logger.log("Error on line {}".format(sys.exc_info()[-1].tb_lineno))

    finally:
        if cursor is not None:
            cursor.close()
            connection_ds.close()


def update_gumtree_listing_for_removed_on(gumtree_ids):
    try:
        connection_ds = mysql.connector.connect(
            host=DB_HOST_DS,
            user=DB_USER_DS,
            password=DB_PASS_DS,
            database=DB_DATABASE_DS
        )
        cursor = connection_ds.cursor()
        converted_list = [str(element) for element in gumtree_ids]
        joined_list = ",".join(converted_list)
        sql = "UPDATE au_gumtree_listings SET removed_on = '{}' WHERE gumtree_Id IN ({})".format(
            str(datetime.now().replace(microsecond=0)), joined_list)
        cursor.execute(sql)
        connection_ds.commit()
        print(cursor.rowcount, "was updated.")
    except Exception as e:
        logger.log("Error in Update_gumtree_listing_for_removed_on: " + str(e))
        logger.log("Error on line {}".format(sys.exc_info()[-1].tb_lineno))

    finally:
        if cursor is not None:
            cursor.close()
            connection_ds.close()


