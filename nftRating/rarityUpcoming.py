import uuid
from time import sleep
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bigQuery import *

# Selenium - browser options
options = Options()
options.headless = True
options.add_argument("--window-size=1920,1200")

# Chromedriver
DRIVER_PATH = './chromedriver'
driver = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)

# Get page from specific URL
driver.get("https://rarity.tools/upcoming")

# Sleep needed to wait till page loaded
sleep(5)
soup = bs(driver.page_source, 'html.parser')

# Get HTML of the page, parsing it and getting the needed data
projects_full_list = []
twitter = None
project_url = None
discord = None
projects = soup.find_all('tr', class_="text-left text-gray-800")
for project in projects:
    projects_full = {}
    project_name = str(project.find('div', 'text-lg font-bold text-pink-700 dark:text-gray-300')).split('600px;">\n\t\t\t')[1].split(
        '<!--')[0]
    try:
        project_description = str(project.find_all('div', attrs={'style': 'max-width: 600px;'})[1]).split('600px;">\n\t\t\t\t')[1].split(
        '\n\t\t\t</div')[0]
    except Exception as ex:
        project_description = ''
        print(ex)
    for idx, a in enumerate(project.find_all('a', href=True)):
        if str(a['href']).__contains__('discord'):
            discord = a['href']
        elif str(a['href']).__contains__('twitter'):
            twitter = str(a['href']).split('twitter.com/')[1]
        else:
            project_url = a['href']
    projects_full.update(
        {"item_name": project_name, "item_type": 1, "description": project_description, "twitter": twitter,
         "project_url": project_url, "discord": discord, "uuid": str(uuid.uuid4())})
    projects_full_list.append(projects_full)
driver.quit()

# Insert rows into raw_data table in BigQuery
insert_rows_from_json(raw_data_table_id, projects_full_list)
