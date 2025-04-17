import mysql.connector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import time
from datetime import datetime
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import threading
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import base64
import requests
import os

logging.basicConfig(filename='errors.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

conn = mysql.connector.connect(
    host="192.168.1.160",
    user="root",
    password="Bhushan@1",
    database="rkd_master",
    port="3306"

    )
print("Connection successful")

cursor = conn.cursor()

def clean_data(value):
    return ''.join(c for c in value if c.isprintable()).strip()

def convert_date_format(date_str):
    date_formats = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y']    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue 
    logging.error(f"Invalid date format: {date_str}")
    return None

# Thread-safe counter implementation
class ThreadSafeCounter:
    def __init__(self):
        self.count = 0
        self.lock = threading.Lock()
    def increment(self):
        with self.lock:
            self.count += 1
    def get_count(self):
        with self.lock:
            return self.count
# Initialize the thread-safe counter
counter1 = ThreadSafeCounter()

def validate_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()  # Assuming date format is YYYY-MM-DD
    except (ValueError, TypeError):
        return None 

db_lock = threading.Lock()

def is_application_number_processed(conn, application_number):
    
    try:
        cursor = conn.cursor()
        query = """
            SELECT toScrape 
            FROM rkd_master.applications 
            WHERE application_number = %s
        """
        time.sleep(0.5)
        cursor.execute(query, (application_number,))
        result = cursor.fetchone()
        
        # Check if toScrape is True (assuming 1 represents True)
        if result and result[0] == 1:
            return True
        return False
    except Exception as e:
        logging.error(f"Error checking application number {application_number}: {e}")
        return False
    finally:
        cursor.close()

def solve_captcha(driver):
    while driver.find_elements(By.ID, 'CaptchaText'):
        try:
            captcha_element = driver.find_element(By.ID, 'CaptchaText')
            driver.find_element(By.ID, 'Captcha').screenshot('captcha.png')

            with open('captcha.png', 'rb') as file:
                encoded_string = base64.b64encode(file.read()).decode('ascii')
                captcha_url = 'https://api.apitruecaptcha.org/one/gettext'
                data = {
                    'userid': 'bhushan.walunj@rkdewanmail.com',
                    'apikey': 'sezDmBsxkTS7h2yxNQf4',
                    'data': encoded_string
                }
                response = requests.post(url=captcha_url, json=data)
                code = response.json().get('result', '')
                
                if not code:
                    print("CAPTCHA solving failed or empty result, retrying...")
                    continue
                
                print(f"Captcha solved: {code}")
                
                captcha_element.clear()
                captcha_element.send_keys(code)
                driver.find_element(By.NAME, 'submit').click()
                
                time.sleep(2)

                error_check = driver.find_elements(By.TAG_NAME, 'h2')
                if error_check and 'Sorry' in error_check[0].text:
                    print('IPO server error after CAPTCHA submission, waiting 3 seconds before retrying...')
                    time.sleep(3) 
                    driver.refresh()
                    time.sleep(2) 
                    continue

                if not driver.find_elements(By.ID, 'CaptchaText'):
                    return True

        except Exception as e:
            print(f"Error during CAPTCHA solving: {e}")
            time.sleep(2)
            continue
    
    return False 

def login_and_navigate(driver):
    try:
        driver.get("https://iprsearch.ipindia.gov.in/publicsearch")
        
        # Wait for the date fields to be present
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "FromDate"))
        )
        
        # Set the from and to dates (you can modify these dates as needed)
        from_date = "01/01/2010"  # Format: MM/dd/yyyy
        to_date = "12/31/2010"    # Format: MM/dd/yyyy
        
        # Find and fill the date fields
        from_date_field = driver.find_element(By.ID, "FromDate")
        to_date_field = driver.find_element(By.ID, "ToDate")
        
        # Clear existing values and input new dates
        from_date_field.clear()
        from_date_field.send_keys(from_date)
        
        to_date_field.clear()
        to_date_field.send_keys(to_date)

        # Solve captcha and submit form
        if not solve_captcha(driver):
            logging.error("Failed to solve CAPTCHA")
            driver.quit()
            exit(1)
        
        # Wait for the table to be present after successful submission
        WebDriverWait(driver, 50).until(
            EC.presence_of_element_located((By.ID, "tableData"))
        )
        print("Chrome opened successfully")
        
    except TimeoutException:
        logging.error("Timed out while loading the page.")
        driver.quit()
        exit(1)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        driver.quit()
        exit(1)

def insert_scraping_log(conn, year, part, app_count):
    try:
        cursor = conn.cursor()
        query = """
            INSERT INTO scrapping_logs (year, part, count) 
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE count = VALUES(count)
        """
        cursor.execute(query, (year, part, app_count))
        conn.commit()
        print(f"Scraping log inserted successfully for Year {year}, Part {part} with {app_count} applications.")
        logging.info(f"Scraping log inserted successfully for Year {year}, Part {part} with {app_count} applications.")
    except Exception as e:
        logging.error(f"Failed to insert scraping log: {e}")
        conn.rollback()
    finally:
        cursor.close()


#-------DATA INSERTION------
def insert_application_data(conn, data):
    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got {type(data)}: {data}")
        return

    # Validate dates before insertion
    date_of_filing = validate_date(data.get('date_of_filing'))
    publication_date_u_s_11a = validate_date(data.get('publication_date_u_s_11a'))
    priority_date = validate_date(data.get('priority_date'))

    cursor = conn.cursor()
    try:
        cursor.execute('''INSERT INTO rkd_master.applications (
                    application_number, date_of_filing, publication_date_u_s_11a,  priority_date,
                    field_of_invention, title_of_invention, publication_number, publication_type,
                    priority_number, priority_country, classification, abstract ,complete_specification         
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
                ON DUPLICATE KEY UPDATE
                    date_of_filing = VALUES(date_of_filing),priority_date = VALUES(priority_date),
                    publication_date_u_s_11a = VALUES(publication_date_u_s_11a),    
                    field_of_invention = VALUES(field_of_invention),title_of_invention = VALUES(title_of_invention),
                    publication_number = VALUES(publication_number),publication_type = VALUES(publication_type),
                    priority_number = VALUES(priority_number),priority_country = VALUES(priority_country),
                    classification = VALUES(classification),abstract = VALUES(abstract),
                    complete_specification = VALUES(complete_specification)
        ''', (
            data.get('application_number',None), date_of_filing, publication_date_u_s_11a,
            priority_date, data.get('field_of_invention', None), data.get('title_of_invention', None),
            data.get('publication_number', None), data.get('publication_type', None),
            data.get('priority_number', None), data.get('priority_country', None),
            data.get('classification', None), data.get('abstract', None), data.get('complete_specification',None)
        ))
        conn.commit()
    except Exception as e:
        logging.error(f"Error inserting data into applications table: {e}")
        conn.rollback()  

def insert_inventors_data(conn, inventors, application_number):
    print("into inventors")
    cursor = conn.cursor()
    for inventor in inventors:
        inventor['inventor_name'] = clean_data(inventor['inventor_name'])
        inventor['inventor_address'] = clean_data(inventor['inventor_address'])
        inventor['inventor_country'] = clean_data(inventor['inventor_country'])
        inventor['inventor_nationality'] = clean_data(inventor['inventor_nationality'])
    try:
        cursor.executemany('''
            INSERT INTO rkd_master.inventors (
                application_number, inventor_name, inventor_address, inventor_country, inventor_nationality
            ) VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE  
            inventor_address = VALUES(inventor_address),inventor_country = VALUES(inventor_country),
            inventor_nationality = VALUES(inventor_nationality)
        ''', [
            (
                application_number, inventor['inventor_name'], inventor['inventor_address'],
                inventor['inventor_country'], inventor['inventor_nationality']
            ) for inventor in inventors
        ])
        conn.commit()

    except Exception as e:
        logging.error(f"Error processing inventors data: {e}")
       
def insert_applicants_data(conn, applicants, application_number):
    print("into applicants")
    cursor = conn.cursor()
    for applicant in applicants:
        applicant['applicant_name'] = clean_data(applicant['applicant_name'])
        applicant['applicant_address'] = clean_data(applicant['applicant_address'])
        applicant['applicant_country'] = clean_data(applicant['applicant_country'])
        applicant['applicant_nationality'] = clean_data(applicant['applicant_nationality'])
    try:
        cursor.executemany('''
            INSERT INTO rkd_master.applicants (
                application_number, applicant_name, applicant_address, applicant_country, applicant_nationality
            ) VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE    
            applicant_address = VALUES(applicant_address),applicant_country = VALUES(applicant_country),
            applicant_nationality = VALUES(applicant_nationality)
        ''', [
            (
                application_number, applicant['applicant_name'], applicant['applicant_address'],
                applicant['applicant_country'], applicant['applicant_nationality']
            ) for applicant in applicants
        ])
        conn.commit()
        
    except Exception as e:
        logging.error(f"Error processing applicant data: {e}")
        
def insert_status_data(conn, data, application_number):
    print("into status data")
    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got {type(data)}: {data}")
        return  
        
    post_grant_journal_date = validate_date(data.get('post_grant_journal_date'))
    pct_international_filing_date = validate_date( data.get('pct_international_filing_date'))
    parent_application_filing_date = validate_date(data.get('parent_application_filing_date'))
    first_examination_report_date = validate_date( data.get('first_examination_report_date'))
    request_for_examination_date = validate_date(data.get('request_for_examination_date'))
    date_of_cert_issue =validate_date(data.get('date_of_cert_issue'))
    
    cursor = conn.cursor()
    try:
        query = '''
            INSERT INTO rkd_master.applications(
                application_number, applicant_name, application_type, email_as_per_record, additional_email,
                email_updated_online, request_for_examination_date, first_examination_report_date,
                date_of_cert_issue, post_grant_journal_date, reply_to_fer_date, application_status,
                pct_international_application_number, pct_international_filing_date,
                parent_application_number, parent_application_filing_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
            ON DUPLICATE KEY UPDATE
                applicant_name = VALUES(applicant_name),application_type = VALUES(application_type),
                email_as_per_record = VALUES(email_as_per_record),
                additional_email = VALUES(additional_email),email_updated_online = VALUES(email_updated_online),
                request_for_examination_date = VALUES(request_for_examination_date),
                first_examination_report_date = VALUES(first_examination_report_date),
                date_of_cert_issue = VALUES(date_of_cert_issue),
                post_grant_journal_date = VALUES(post_grant_journal_date),
                reply_to_fer_date = VALUES(reply_to_fer_date), application_status = VALUES(application_status),
                pct_international_application_number = VALUES(pct_international_application_number),
                pct_international_filing_date = VALUES(pct_international_filing_date),
                parent_application_number = VALUES(parent_application_number),
                parent_application_filing_date = VALUES(parent_application_filing_date)
        '''
        values = (
            application_number, data.get('applicant_name', None), data.get('application_type', None),
            data.get('email_as_per_record', None),data.get('additional_email', None), 
            data.get('email_updated_online', None), request_for_examination_date, first_examination_report_date,
            date_of_cert_issue,post_grant_journal_date, data.get('reply_to_fer_date', None),
            data.get('application_status', None),data.get('pct_international_application_number', None), 
            pct_international_filing_date,data.get('parent_application_number', None), parent_application_filing_date
        )
        cursor.execute(query, values)
        conn.commit()
        logging.info(f"Data successfully inserted for application number {application_number}")
    
    except Exception as e:
        logging.error(f"Error inserting data into status table: {e}")
        conn.rollback()

def insert_combined_data(conn, g1_data, g2_data, g3_data, g4_data, g5_data, g6_data , application_number):
    try:
        if g1_data and isinstance(g1_data, dict):
            cursor = conn.cursor()

            # Prepare query with optional fields and values
            query = '''
            INSERT INTO rkd_master.renewal (
                application_number, due_date_of_next_renewal, legal_patent_status, date_of_cessation
            ) VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                due_date_of_next_renewal = VALUES(due_date_of_next_renewal),
                legal_patent_status = VALUES(legal_patent_status),
                date_of_cessation = VALUES(date_of_cessation)
            '''
            values = [
                application_number,
                g1_data.get('due_date_of_next_renewal', None),
                g1_data.get('legal_patent_status', None),
                g1_data.get('date_of_cessation', None)
            ]

            cursor.execute(query, values)
            conn.commit()


        if g1_data and isinstance(g1_data, dict): 
            cursor = conn.cursor()                      
            query = '''
            INSERT INTO rkd_master.applications (application_number
            '''
            values = [application_number]
            updates = []

            # Conditionally add fields to the query
            if 'due_date_of_next_renewal' in g1_data:
                query += ', due_date_of_next_renewal'
                values.append(g1_data['due_date_of_next_renewal'])
                updates.append('due_date_of_next_renewal = VALUES(due_date_of_next_renewal)')
            if 'legal_patent_status' in g1_data:
                query += ', legal_patent_status'
                values.append(g1_data['legal_patent_status'])
                updates.append('legal_patent_status = VALUES(legal_patent_status)')
            if 'date_of_cessation' in g1_data:
                query += ', date_of_cessation'
                values.append(g1_data['date_of_cessation'])
                updates.append('date_of_cessation = VALUES(date_of_cessation)')

            # Finalize the query
            query += ') VALUES (%s' + ', %s' * (len(values) - 1) + ') '
            if updates:
                query += 'ON DUPLICATE KEY UPDATE ' + ', '.join(updates)
            
            cursor.execute(query, values)
            conn.commit()

        if not isinstance(g2_data, dict) or not isinstance(g3_data, dict):
            logging.error(f"Expected dictionaries but got g1_data: {type(g1_data)}, g2_data: {type(g2_data)}, g3_data: {type(g3_data)}")
            return
        query = '''
            INSERT INTO rkd_master.renewal (
                application_number,
                patent_number, date_of_patent, date_of_grant, application_type, date_of_recordal,
                parent_application_number, appropriate_office, pct_international_application_number,
                pct_international_filing_date, grant_title, address_of_service, additional_address_of_service, priority_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                patent_number = VALUES(patent_number),date_of_patent = VALUES(date_of_patent),
                date_of_grant = VALUES(date_of_grant), application_type = VALUES(application_type),
                date_of_recordal = VALUES(date_of_recordal),
                parent_application_number = VALUES(parent_application_number),
                appropriate_office = VALUES(appropriate_office),
                pct_international_application_number = VALUES(pct_international_application_number),
                pct_international_filing_date = VALUES(pct_international_filing_date),
                grant_title = VALUES(grant_title), address_of_service = VALUES(address_of_service),
                additional_address_of_service = VALUES(additional_address_of_service),
                priority_date = VALUES(priority_date)
        '''
        values = (
            application_number,
            g2_data.get('patent_number', None), g2_data.get('date_of_patent', None),
            g2_data.get('date_of_grant', None),g2_data.get('application_type', None), 
            g2_data.get('date_of_recordal', None), g2_data.get('parent_application_number', None),
            g2_data.get('appropriate_office', None), g2_data.get('pct_international_application_number', None),
            g2_data.get('pct_international_filing_date', None), g2_data.get('grant_title', None),
            g3_data.get('address_of_service', None), g3_data.get('additional_address_of_service', None),
            g3_data.get('priority_date', None)
        )
        cursor.execute(query, values)
        conn.commit()

        # insert renewal data into applications table
        query = '''
            INSERT INTO rkd_master.applications (
                application_number, patent_number, date_of_patent, date_of_grant,
                application_type, appropriate_office, address_of_service
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE            
                    patent_number = VALUES(patent_number),date_of_patent = VALUES(date_of_patent),
                    date_of_grant = VALUES(date_of_grant),application_type = VALUES(application_type), 
                    appropriate_office = VALUES(appropriate_office),address_of_service = VALUES(address_of_service)                                                                      
            '''
        values = (
                application_number,
                g2_data.get('patent_number', None), g2_data.get('date_of_patent',None),
                g2_data.get('date_of_grant',None), g2_data.get('application_type', None),
                g2_data.get('appropriate_office', None), g3_data.get('address_of_service', None)
            )
        cursor.execute(query, values)
        conn.commit()

        if g4_data:
            if not isinstance(g4_data, list):
                logging.error(f"Expected a list but got {type(g4_data)}: {g4_data}")
                return
            cursor = conn.cursor()  
            query = '''
                INSERT INTO rkd_master.renewal_fee_data (
                    application_number, year, normal_due_date, due_date_with_extension, cbr_no, 
                    cbr_date, renewal_amount, renewal_certificate_number, date_of_renewal, 
                    renewal_period_from, renewal_period_to
                ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    
                    normal_due_date = VALUES(normal_due_date), due_date_with_extension = VALUES(due_date_with_extension), 
                    cbr_no = VALUES(cbr_no), cbr_date = VALUES(cbr_date), renewal_amount = VALUES(renewal_amount),
                    renewal_certificate_number = VALUES(renewal_certificate_number), 
                    date_of_renewal = VALUES(date_of_renewal), renewal_period_from = VALUES(renewal_period_from), 
                    renewal_period_to = VALUES(renewal_period_to)
            '''
            for row in g4_data:
                values = (
                    application_number, row.get('Year', None),
                    convert_date_format(row.get('Normal Due Date', None)),  
                    convert_date_format(row.get('Due Date with Extension', None)), 
                    row.get('CBR No', None), convert_date_format(row.get('CBR Date', None)),  
                    row.get('Renewal Amount', None), row.get('Renewal Certificate No', None),
                    convert_date_format(row.get('Date of Renewal', None)),  
                    convert_date_format(row.get('From', None)), convert_date_format(row.get('To', None)),  
                )
                cursor.execute(query, values)
                conn.commit()
        
        if g5_data:
            print("into g5")
            if not isinstance(g5_data, list):
                logging.error(f"Expected a list but got {type(g5_data)}: {g5_data}")
                return
            cursor = conn.cursor()
            query = '''
                INSERT INTO rkd_master.grantee_data (
                    application_number, sl_no, name_of_grantee, grantee_type, address_of_grantee
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE         
                    sl_no = VALUES(sl_no), grantee_type = VALUES(grantee_type),
                    address_of_grantee = VALUES(address_of_grantee)
            '''
            for grantee in g5_data:
                try:
                    cursor.execute(query, (
                        application_number, grantee['sl_no'],grantee['name_of_grantee'], 
                        grantee['grantee_type'], grantee['address_of_grantee']
                    ))
                    conn.commit()
                except Exception as e:
                    print(f"Error inserting grantee: {e}")
              
        if g6_data:
            print("into g6")
            if not isinstance(g6_data, list):
                logging.error(f"Expected a list but got {type(g6_data)}: {g6_data}")
                return
            cursor = conn.cursor()
            query = '''
                INSERT INTO rkd_master.patentee_data (
                    application_number, sl_no, name_of_patentee, patentee_type, address_of_patentee
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE         
                    sl_no = VALUES(sl_no),patentee_type = VALUES(patentee_type),
                    address_of_patentee = VALUES(address_of_patentee)
            '''
            for patentee in g6_data:
                try:
                    cursor.execute(query, (
                        application_number, patentee['sl_no'], patentee['name_of_patentee'], 
                        patentee['patentee_type'], patentee['address_of_patentee']
                    ))
                    conn.commit()
                except Exception as e:
                    print(f"Error inserting patentee: {e}")

        logging.info(f"Data successfully inserted/updated for application number {application_number}")

    except Exception as e:
            logging.error(f"Error in inserting data into all table: {e}")
            conn.rollback()
    finally:
            cursor.close()

#-----DATA EXTRACTION FROM BOTH LINKS------
global application_data
application_data = {}
inventors = []
applicants = []
global status_data 
status_data = {}

def extract_data_from_tab(driver, conn, focus):
        global application_data, inventors, applicants, status_data
        original_window = driver.current_window_handle
        logging.info(f"Opened tabs")
        try:
                if focus == "first":
                    
                    try:                       
                        inventors.clear()
                        applicants.clear()
                        application_data.clear()
                        status_data.clear()

                        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "home")))
                        
                        home_div = driver.find_element(By.ID, "home")
                        html = home_div.get_attribute('outerHTML')
                        soup = BeautifulSoup(html, 'html.parser')

                        error_check_1 = driver.find_elements(By.TAG_NAME, 'h2')
                        if error_check_1 and 'Sorry' in error_check_1[0].text:
                            logging.error('IPO server error loading Captcha page, refreshing...')
                            driver.close()
                            driver.switch_to.window(original_window)
                            return
                        
                        # Extract application data
                        table_rows = soup.find_all('tr')
                        for row in table_rows:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                key = cols[0].get_text(strip=True)
                                value = cols[1].get_text(strip=True)
                                if key == "Application Number":
                                    application_data['application_number'] = value
                                    status_data['application_number'] = value
                                elif key == "Application Filing Date":
                                    application_data['date_of_filing'] = convert_date_format(value)
                                elif key == "Publication Number":
                                    application_data['publication_number'] = value
                                elif key == "Publication Type":
                                    application_data['publication_type'] = value
                                elif key == "Priority Number":
                                    application_data['priority_number'] = value
                                elif key == "Priority Date":
                                    application_data['priority_date'] = convert_date_format(value)
                                elif key == "Publication Date":
                                    application_data['publication_date_u_s_11a'] = convert_date_format(value)
                                elif key == "Priority Country":
                                    application_data['priority_country'] = value
                                elif key == "Field Of Invention":
                                    application_data['field_of_invention'] = value
                                elif key == "Invention Title":
                                    application_data['title_of_invention'] = value
                                elif key == "Classification (IPC)":
                                    application_data['classification'] = value

                        # Extract inventors details
                        inventor_table = soup.find_all('table', class_='table-striped')[1]
                        inventor_rows = inventor_table.find_all('tr')[1:]
                        for inventor_row in inventor_rows:
                            cols = inventor_row.find_all('td')
                            if len(cols) >= 4:
                                inventor_data = {
                                    'application_number': application_data.get('application_number', ''),
                                    'inventor_name': cols[0].get_text(strip=True),
                                    'inventor_address': cols[1].get_text(strip=True),
                                    'inventor_country': cols[2].get_text(strip=True),
                                    'inventor_nationality': cols[3].get_text(strip=True)
                                }
                                inventors.append(inventor_data)
                        
                        # Extract applicants details
                        applicant_table = soup.find_all('table', class_='table-striped')[2]
                        applicant_rows = applicant_table.find_all('tr')[1:]  
                        for applicant_row in applicant_rows:
                            cols = applicant_row.find_all('td')
                            if len(cols) >= 4:
                                applicant_data = {
                                    'application_number': application_data.get('application_number', ''),
                                    'applicant_name': cols[0].get_text(strip=True),
                                    'applicant_address': cols[1].get_text(strip=True),
                                    'applicant_country': cols[2].get_text(strip=True),
                                    'applicant_nationality': cols[3].get_text(strip=True)
                                }
                                applicants.append(applicant_data)

                        # Extract abstract and complete specification
                        colspan_tds = soup.find_all('td', colspan='2')
                        if len(colspan_tds) >= 6:
                            abstract = colspan_tds[4]
                            application_data['abstract'] = abstract.get_text(strip=True)
            
                        complete_specification = soup.find('textarea', id='COMPLETE_SPECIFICATION')
                        if complete_specification:
                            application_data['complete_specification'] = complete_specification.get_text(strip=True)                      

                        # Insert data into the database
                        insert_application_data(conn, application_data)
                        insert_inventors_data(conn, inventors, application_data['application_number'])
                        insert_applicants_data(conn,applicants, application_data['application_number'])
                        logging.info(f"Data inserted into database for application number: {application_data.get('application_number')}")
                        counter1.increment()
                        print(f"INSERTED application details of {application_data.get('application_number')} : count - {counter1.get_count()}")

                        logging.info("Closed first tab and switched back to the main window.")
                    except Exception as e:
                        logging.error(f"Error processing first column link: {e}")
                        
                elif focus == "fifth":          
                    try:
                        WebDriverWait(driver, 3).until(EC.number_of_windows_to_be(2))
                        
                        error_check_2 = driver.find_elements(By.TAG_NAME, 'h2')
                        if error_check_2 and 'Sorry' in error_check_2[0].text:
                            logging.error('IPO server error loading Captcha page, refreshing...')
                            driver.close()
                            driver.switch_to.window(original_window)
                            return
                        
                        WebDriverWait(driver, 7).until(
                            EC.visibility_of_element_located((By.XPATH, "//div[@class='tab-pane fade active in Action PatentDetails']"))
                        )
                        divs = driver.find_elements(By.XPATH, "//div[@class='tab-pane fade active in Action PatentDetails']")             
                        if len(divs) > 0:
                            details_div = divs[0]
                            details_html = details_div.get_attribute('outerHTML')
                            details_soup = BeautifulSoup(details_html, 'html.parser')

                            # Extract application status data
                            details_table = details_soup.find('table', class_='table-striped')
                            if details_table:                        
                                for row in details_table.find_all('tr')[1:]:  
                                    cells = row.find_all('td')          
                                    if len(cells) >= 2:
                                        key = cells[0].text.strip()
                                        value = cells[1].text.strip()
                                        status_data['application_number'] = application_data.get('application_number', '')
                                        if key == "APPLICANT NAME":
                                            status_data['applicant_name'] = value
                                        elif key == "APPLICATION TYPE":
                                            status_data['application_type'] = value
                                        elif key == "E-MAIL (As Per Record)":
                                            status_data['email_as_per_record'] = value
                                        elif key == "ADDITIONAL-EMAIL (As Per Record)":
                                            status_data['additional_email'] = value
                                        elif key == "E-MAIL (UPDATED Online)":
                                            status_data['email_updated_online'] = value
                                        elif key == "PCT INTERNATIONAL APPLICATION NUMBER":
                                            status_data['pct_international_application_number'] = value
                                        elif key == "PCT INTERNATIONAL FILING DATE":
                                            status_data['pct_international_filing_date'] = convert_date_format(value)
                                        elif key == "PARENT APPLICATION NUMBER":
                                            status_data['parent_application_number'] = value
                                        elif key == "PARENT APPLICATION FILING DATE":
                                            status_data['parent_application_filing_date'] = convert_date_format(value)
                                        elif key == "REQUEST FOR EXAMINATION DATE":
                                            status_data['request_for_examination_date'] = convert_date_format(value)
                                        elif key == "FIRST EXAMINATION REPORT DATE":
                                            status_data['first_examination_report_date'] = convert_date_format(value)
                                        elif key == "Date Of Certificate Issue":
                                            status_data['date_of_cert_issue'] = convert_date_format(value)
                                        elif key == "POST GRANT JOURNAL DATE":
                                            status_data['post_grant_journal_date'] = convert_date_format(value)
                                        elif key == "REPLY TO FER DATE":
                                            status_data['reply_to_fer_date'] = convert_date_format(value)
                            
                        if len(divs) > 1:
                            status_div = divs[1]
                            status_html = status_div.get_attribute('outerHTML')
                            status_soup = BeautifulSoup(status_html, 'html.parser')
                            status_tables = status_soup.find_all('table', class_='table-striped')
                            status_table = status_tables[0]

                            # Extract application status of a particular application number
                            if status_table:
                                for row in status_table.find_all('tr')[1:]:  
                                    cells = row.find_all('td')
                                    if len(cells) >= 2:
                                        key = cells[0].text.strip()
                                        value = cells[1].text.strip()
                                        if key == "APPLICATION STATUS":
                                            granted_status = value.split(',')[0].strip()
                                            if "Granted Application" in granted_status:
                                                status_data['application_status'] = granted_status #for granted
                                            else:
                                                status_data['application_status'] = value    
                    
                        status_data['application_number'] = application_data.get('application_number', '')
                        application_num =  status_data['application_number']
                        
                        # Insert data into the database
                        insert_status_data(conn, status_data, application_num)  
                        logging.info(f"Data inserted into database for status data.")

                        # Search for eregister button
                        if len(status_tables) > 1:
                            original_window = driver.current_window_handle
                            status_div = divs[1]
                            eregister_button = WebDriverWait(driver, 10).until(
                                EC.element_to_be_clickable((By.XPATH, "//input[@name='SubmitAction' and @value='E-Register']")))
                                                            
                            if eregister_button is not None:
                                eregister_button.click()
                                all_data = extract_all_data(driver)

                                # Insert all extracted renewal data
                                if all_data['g1'] or all_data['g2'] or all_data['g3'] or all_data['g4'] or all_data['g5'] or all_data['g6']:
                                    time.sleep(2) 
                                    insert_combined_data(conn, all_data['g1'], all_data['g2'], all_data['g3'], all_data['g4'],all_data['g5'],all_data['g6'], application_num)
                                    
                                    logging.info(f"Data inserted into database for renewal data.")
                                else:
                                    print("No data extracted to insert.")
                        
                        driver.switch_to.window(original_window)
                        logging.info("Closed status tab and switched back to the main window.")
                    except Exception as e:
                        logging.error(f"Error processing fifth column link: {e}")       
                else:
                    logging.error("Invalid focus option provided.")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            

def extract_all_data(driver):
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "tab-content")))
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # Initialize a dictionary to hold all extracted data
    all_data = {
        'g1': {}, 'g2': {}, 'g3': {},
        'g4': [], 'g5': [], 'g6': []
    }
    # Extract granted status data (g1)
    tables = soup.find_all('table', class_='Default')
    if len(tables) > 0:
        table1 = tables[0]  
        rows = table1.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) == 2:
                key = cols[0].get_text(strip=True).replace(" :", "")
                value = cols[1].get_text(strip=True)
                if key == "Legal Status":
                    all_data['g1']['legal_patent_status'] = value
                elif key == "Due date of next renewal":
                    all_data['g1']['due_date_of_next_renewal'] = convert_date_format(value)
                elif key == 'Date Of Cessation':
                    all_data['g1']['date_of_cessation'] = convert_date_format(value)

    # Extract granted patent data (g2)
    detail_tables = soup.find_all('table', class_='table-striped')
    if len(detail_tables) > 0:
        table2 = detail_tables[0]  
        rows = table2.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 6:  # Handling the normal rows with two data fields
                field1 = cols[0].get_text(strip=True)
                value1 = cols[2].get_text(strip=True)
                if field1 == "Patent Number":
                    all_data['g2']['patent_number'] = value1
                elif field1 == "Application Number":
                    all_data['g2']['application_number'] = value1
                elif field1 == "Type of Application":
                    all_data['g2']['application_type'] = value1
                elif field1 == "Parent Application Number":
                    all_data['g2']['parent_application_number'] = value1
                elif field1 == "PCT International Application Number":
                    all_data['g2']['pct_international_application_number'] = value1

                field2 = cols[3].get_text(strip=True)
                value2 = cols[5].get_text(strip=True)
                if field2 == "Date of Patent":
                    all_data['g2']['date_of_patent'] = convert_date_format(value2)
                elif field2 == "Date of Grant":
                    all_data['g2']['date_of_grant'] = convert_date_format(value2)
                elif field2 == "Date of Recordal":
                    all_data['g2']['date_of_recordal'] = convert_date_format(value2)
                elif field2 == "Appropriate Office":
                    all_data['g2']['appropriate_office'] = value2
                elif field2 == "PCT International Filing Date":
                    all_data['g2']['pct_international_filing_date'] = convert_date_format(value2)
                
            elif len(cols) >= 3:
                field = cols[0].get_text(strip=True)
                value = cols[2].get_text(strip=True)
                if field == "Grant Title":
                    all_data['g2']['grant_title'] = value

    # Extract address data (g3)
    tables = soup.find_all('table', class_='Default')
    if len(tables) >= 1:  
        table3 = tables[1]
        rows = table3.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 3:  
                key = cols[0].get_text(strip=True).replace(" :", "")
                value = cols[2].get_text(strip=True)
                if key == "Address of Service":
                    all_data['g3']['address_of_service'] = value
                elif key == "Additional Address of Service":
                    all_data['g3']['additional_address_of_service'] = value
                elif key == "Priority Date":
                    all_data['g3']['priority_date'] = convert_date_format(value)
    
    # Extract renewal fee data (g4)
    if len(detail_tables) > 4:
        table4 = detail_tables[3] 
        headers = table4.find_all('th')
        header_row = headers[0:3] + headers[5:8] + headers[9:11]
        header_labels = [header.text.strip() for header in header_row]
        
        rows = table4.find_all('tr')[2:]  
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 10:
                row_data = {
                    'Year': cols[0].text.strip(),
                    'Normal Due Date': cols[1].text.strip(),
                    'Due Date with Extension': cols[2].text.strip(),
                    'CBR No': cols[3].text.strip(),
                    'CBR Date': cols[4].text.strip(),
                    'Renewal Amount': cols[5].text.strip(),
                    'Renewal Certificate No': cols[6].text.strip(),
                    'Date of Renewal': cols[7].text.strip(),
                    'From': cols[8].text.strip(),
                    'To': cols[9].text.strip()
                }
                all_data['g4'].append(row_data)

     # Extract grantee data (g5)
    if len(detail_tables) > 2:
        table5 = detail_tables[1]
        grantee_rows = table5.find_all('tr')[1:]
        for grantee_row in grantee_rows:
            cols = grantee_row.find_all('td')
            if len(cols) >= 4:
                grantee_data = {
                    'sl_no': cols[0].get_text(strip=True),
                    'name_of_grantee': cols[1].get_text(strip=True),
                    'grantee_type': cols[2].get_text(strip=True),
                    'address_of_grantee': cols[3].get_text(strip=True)
                }
                all_data['g5'].append(grantee_data)
    
    # Extract patentee data (g6)
    if len(detail_tables) >= 4:
        table5 = detail_tables[2]
        patentee_rows = table5.find_all('tr')[1:]
        for patentee_row in patentee_rows:
            cols = patentee_row.find_all('td')
            if len(cols) >= 4:
                patentee_data = {
                    'sl_no': cols[0].get_text(strip=True),
                    'name_of_patentee': cols[1].get_text(strip=True),
                    'patentee_type': cols[2].get_text(strip=True),
                    'address_of_patentee': cols[3].get_text(strip=True)
                }
                all_data['g6'].append(patentee_data)
    return all_data


def open_and_process_links(driver, conn, link, focus):
    original_window = driver.current_window_handle

    try:
        open_link_in_new_tab(driver, link)
        time.sleep(2) 
        extract_data_from_tab(driver, conn, focus=focus)

    except Exception as e:
        logging.error(f"Error processing link ({focus} column): {e}")
    finally:
        new_window_handle = [handle for handle in driver.window_handles if handle != original_window]
        if new_window_handle:
            driver.switch_to.window(new_window_handle[0])
            driver.close()
        driver.switch_to.window(original_window)

def process_page(driver, conn):
    try:
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, 'tableData')))
        table = driver.find_element(By.ID, "tableData")
        rows = table.find_elements(By.TAG_NAME, "tr")

        for row in rows[1:]:  
            columns = row.find_elements(By.TAG_NAME, "td")
            if len(columns) >= 5:
                try:
                    application_button = columns[0].find_element(By.TAG_NAME, "button")
                    application_number = application_button.get_attribute("value").strip()
                    if not is_application_number_processed(conn, application_number):
                        print(f"Application number {application_number} not in database. Skipping.")
                        continue

                    first_column_links = columns[0].find_element(By.TAG_NAME, "button")
                    fifth_column_links = columns[4].find_element(By.TAG_NAME, "button")

                    open_and_process_links(driver, conn, first_column_links, focus="first")
                    open_and_process_links(driver, conn, fifth_column_links, focus="fifth")
                except NoSuchElementException:
                    logging.warning("No button found in one of the columns.")
                    continue
        try:
            next_button = driver.find_element(By.CLASS_NAME, "next")
            if not next_button.is_enabled():
                return False
            next_button.click()
            print("NEXT page")
           
            WebDriverWait(driver, 15).until(EC.staleness_of(next_button))
            time.sleep(15)
        except NoSuchElementException:
            logging.info("No 'Next' button found, exiting loop.")
            return False
        return True

    except Exception as e:
        logging.error(f"Error processing page: {e}")
        return False


def open_link_in_new_tab(driver, link):
    link.send_keys(Keys.CONTROL + Keys.RETURN)
    # time.sleep(2) 
    WebDriverWait(driver, 3).until(lambda d: len(d.window_handles) > 1)
    new_window_handle = [handle for handle in driver.window_handles if handle != driver.current_window_handle][0]
    driver.switch_to.window(new_window_handle)

def insert_scraping_log(conn, year, app_count):
    try:
        cursor = conn.cursor()
        query = """
            INSERT INTO scrapping_logs (year, count) 
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE count = VALUES(count)
        """
        cursor.execute(query, (year, app_count))
        conn.commit()
        print(f"Scraping log inserted successfully for Year {year} with {app_count} applications.")
        logging.info(f"Scraping log inserted successfully for Year {year} with {app_count} applications.")
    except Exception as e:
        logging.error(f"Failed to insert scraping log: {e}")
        conn.rollback()
    finally:
        cursor.close()

def main():
    # Extract year and semester from filename
    filename = os.path.basename(__file__)
    year = filename.split('_')[0]
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--start-maximized")
    options.add_argument("--enable-logging")
    options.add_argument('--disable-software-rasterizer')
    options.add_argument("--disable-web-security")
    # options.add_argument('--headless')  # Add headless mode
    options.add_argument('--window-size=1920,1080')  
    options.add_argument('--disable-background-timer-throttling')
    options.add_argument('--disable-backgrounding-occluded-windows')
    options.add_argument('--disable-renderer-backgrounding')
    service = Service("C:/Users/RKD-P23/Desktop/scrapping/chromedriver-win64/chromedriver.exe")
    driver = webdriver.Chrome(service=service, options=options)

    try:
        login_and_navigate(driver)
        while process_page(driver, conn):
            pass  # Continue processing pages until there are no more
        # Insert log after scraping completion
        # print(f"Scraping completed for Year {year} Semester {semester}")
        scraped_count = counter1.get_count()

        # Insert log after scraping completion
        print(f"Scraping completed for Year {year} with {scraped_count} applications.")
        insert_scraping_log(conn, year, scraped_count)


    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if 'driver' in locals():
            driver.quit()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
