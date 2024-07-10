import scrapy,json
from scrapy.http import Request
import pandas as pd
from datetime import datetime
import io,time
import urllib.parse
import requests
import copy
from scrapy import Selector
import mysql.connector

import logging
logger = logging.getLogger(__name__)

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import scoped_session
from scrapy.crawler import proxies

from suntransfers.models import Batch2Input1
# Assuming you already have an engine
engine = create_engine('mysql+pymysql://u413107573_suntransfer_nw:Suntransfer2024@srv945.hstgr.io/u413107573_suntransfer_nw',
                        pool_size=10,          # Adjust pool size as needed
                        max_overflow=20,       # Adjust max overflow as needed
                        pool_recycle=900,     # Recycle connections every hour
                        pool_pre_ping=True  )
Session = sessionmaker(bind=engine)
session = scoped_session(Session)

class SuntransferPriceSpider(scrapy.Spider):
    name = "suntransfer_price_try1"
    start_urls = ["https://www.suntransfers.com/"]

    input_date = "23-07-2024 10:00"
    dt_object = datetime.strptime(input_date, '%d-%m-%Y %H:%M')

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://booking.suntransfers.com',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    }

    booking = {
        'f_outbound_day': '23',
        'f_outbound_month': f"{dt_object.month}-{dt_object.year}",
        'f_outbound_date': dt_object.strftime('%d/%m/%Y'),
        'f_outbound_hours': '10',
        'f_outbound_minutes': '00',
        'f_outbound_time': dt_object.strftime('%H:%M')
    }

    payload = {
        'booking[form_get_quote_now_l]': '',
        'booking[f_departure]': '26134',
        'booking[a_departure][id]': '',
        'booking[a_departure][cod]': '',
        'booking[f_arrival]': '25077',
        'booking[a_arrival][id]': '',
        'booking[a_arrival][cod]': '',
        'booking[f_fromto]': 'ar_1',
        'booking[f_outbound_day]': booking.get('f_outbound_day'),
        'booking[f_outbound_month]': booking.get('f_outbound_month'),
        'booking[f_outbound_date]': booking.get('f_outbound_date'),
        'booking[f_outbound_hours]': booking.get('f_outbound_hours'),
        'booking[f_outbound_minutes]': booking.get('f_outbound_minutes'),
        'booking[f_return_day]': '',
        'booking[f_return_month]': '',
        'booking[f_return_date]': '',
        'booking[f_return_hours]': '',
        'booking[f_return_minutes]': '',
        'booking[f_return_time]': '',
        'booking[f_pax]': '2',
        'booking[f_adults]': '2',
        'booking[f_children]': '0',
        'booking[f_infants]': '0',
        'searchDateTime': '',
        'step': '1',
        'booking[f_outbound_time]': booking.get('f_outbound_time')
    }

    def __init__(self, *args, **kwargs):
        super(SuntransferPriceSpider, self).__init__(*args, **kwargs)
        self.batch_size = 100
        self.item_count = 0
        self.mysql_config = {
            'user': 'u413107573_suntransfer_nw',
            'password': 'Suntransfer2024',
            'host': 'srv945.hstgr.io',
            'database': 'u413107573_suntransfer_nw',
            'connect_timeout': 28800,
        }
        self.connect_mysql()

    def connect_mysql(self):
        self.conn = mysql.connector.connect(**self.mysql_config)
        self.cursor = self.conn.cursor(dictionary=True)

    def close_mysql(self):
        self.cursor.close()
        self.conn.close()

    def get_records_with_conditions(self, batch_size):
        try:
            rows = session.query(Batch2Input1).filter(
                Batch2Input1.status == None,
                Batch2Input1.Retry < 2
            ).limit(batch_size).all()
            logger.info("Query executed successfully")
            return rows
        except Exception as e:
            logger.error("Error executing query: %s", str(e))
            return []

    def parse(self, response):
        batch_number = 0
        while True:
            records = self.get_records_with_conditions(self.batch_size)
            if not records:
                break
            batch_number += 1

            for i, row in enumerate(records, 1):
                try:
                    output_data = self.parser_data(row)
                    self.save_to_mysql(output_data, i)
                    yield output_data

                    self.item_count += 1
                    if self.item_count >= 30:
                        logger.info("Reached 320 items. Pausing for a minute...")
                        time.sleep(30)
                        self.item_count = 0
                        break  # Break out of the inner loop to restart the spider

                except Exception as e:
                    logger.error(f"Error in row {row.route_start}_{row.route_dest}: {e}")
            session.commit()

    def save_to_mysql(self, data, counter):
        try:
            with session.no_autoflush:
                record = session.query(Batch2Input1).filter(
                    Batch2Input1.from_alternateId == data.get('from_alternateId'),
                    Batch2Input1.to_alternateId == data.get('to_alternateId')
                ).one_or_none()

                if record:
                    record.pax_1 = data.get('pax_1')
                    record.pax_2 = data.get('pax_2')
                    record.pax_3 = data.get('pax_3')
                    record.pax_4 = data.get('pax_4')
                    record.pax_5 = data.get('pax_5')
                    record.pax_6 = data.get('pax_6')
                    record.pax_7 = data.get('pax_7')
                    record.pax_8 = data.get('pax_8')
                    record.pax_9 = data.get('pax_9')
                    record.pax_10 = data.get('pax_10')
                    record.pax_11 = data.get('pax_11')
                    record.pax_12 = data.get('pax_12')
                    record.pax_13 = data.get('pax_13')
                    record.pax_14 = data.get('pax_14')
                    record.pax_15 = data.get('pax_15')
                    record.pax_16 = data.get('pax_16')
                    record.Retry = data.get('Retry', 0)
                    record.status = data.get('status')
                    session.commit()
                else:
                    print("Record not found.")
        except OperationalError as e:
            session.rollback()
            print(f"OperationalError encountered: {e}")
        except Exception as e:
            session.rollback()
            print(f"An error occurred: {e}")

    def parser_data(self, row):
        from_alternateId = row.from_alternateId
        to_alternateId = row.to_alternateId
        route_start = row.route_start
        route_dest = row.route_dest
        retry = row.Retry
        output_data = {}
        from_id = int(from_alternateId)
        to_id = int(to_alternateId)
        aiport_code = row.CODE
        url = f"https://booking.suntransfers.com/booking?step=1&iata={aiport_code}&fromNoMatches=0"
        temp_payload = copy.deepcopy(self.payload)

        temp_payload['booking[f_departure]'] = from_id
        temp_payload['booking[f_arrival]'] = to_id

        stored_pax_values = []
        x_paxs = {i: [] for i in range(1, 17)}
        for i in range(1, 17):
            if i in stored_pax_values:
                continue
            temp_payload['booking[f_pax]'] = str(i)
            temp_payload['booking[f_adults]'] = str(i)

            data = requests.post(url, headers=self.headers, data=temp_payload)
            response = Selector(text=data.text)
            no_results = response.xpath('//text()[contains(.,"We are very sorry, unfortunately we are not able to offer you")]').get()
            if no_results:
                break
            vehicle_lst = response.xpath('//*[contains(@id,"vehicle_list_item")]')

            for vehicle in vehicle_lst:
                pax = vehicle.xpath('.//text()[contains(.,"Up to") and contains(.,"passengers")]').get()
                if pax:
                    pax = pax.replace('Up to ', '').replace(' passengers', '').strip()
                    stored_pax_values.append(int(pax))
                    if int(pax) < 16:
                        price = vehicle.xpath('.//*[@class="c-pricing__pricing"]//text()[contains(.,"€")]').get()
                        if price:
                            price = price.replace('€', '').strip()
                            x_paxs[int(pax)].append(price)

        lowest_values = {}
        value_status = False
        for passengers, prices in x_paxs.items():
            if prices:
                lowest_values[passengers] = min(prices)
                value_status = True
            else:
                lowest_values[passengers] = None
        if value_status:
            output_data['status'] = True
        else:
            output_data['status'] = None
            try:
                retry = int(retry)
                retry = retry + 1
                output_data['Retry'] = retry
            except:
                output_data['Retry'] = 1

        output_data['pax_1'] = lowest_values.get(1)
        output_data['pax_2'] = lowest_values.get(2)
        output_data['pax_3'] = lowest_values.get(3)
        output_data['pax_4'] = lowest_values.get(4)
        output_data['pax_5'] = lowest_values.get(5)
        output_data['pax_6'] = lowest_values.get(6)
        output_data['pax_7'] = lowest_values.get(7)
        output_data['pax_8'] = lowest_values.get(8)
        output_data['pax_9'] = lowest_values.get(9)
        output_data['pax_10'] = lowest_values.get(10)
        output_data['pax_11'] = lowest_values.get(11)
        output_data['pax_12'] = lowest_values.get(12)
        output_data['pax_13'] = lowest_values.get(13)
        output_data['pax_14'] = lowest_values.get(14)
        output_data['pax_15'] = lowest_values.get(15)
        output_data['pax_16'] = lowest_values.get(16)
        output_data['from_alternateId'] = from_alternateId
        output_data['to_alternateId'] = to_alternateId
        output_data['route_start'] = route_start
        output_data['route_dest'] = route_dest

        return output_data